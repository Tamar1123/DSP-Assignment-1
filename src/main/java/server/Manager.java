package server;
import utils.AWS;
import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

// Routes client work to workers and answers each client once its images are OCRed.
// Two threads: the main Router receives and admits, one IoWorker does all blocking calls.
// The manager never OCRs or downloads images.
public class Manager implements Runnable {
    // Set by -D; also on with the client debug flag (tests).
    static volatile boolean debug = false;
    private static final int RECEIVE_MAX = 10;
    private static final int RECEIVE_WAIT_SECONDS = 1;
    // Visibility delay for a NEW_TASK a full manager gives back, so it is not busy-looped.
    private static final int REQUEUE_DELAY_SECONDS = 5;
    // How often run() reconciles the worker set and launches crash replacements.
    private static final long WORKER_RECONCILE_INTERVAL_MILLIS = 10000L;

    // Max EC2 instances (managers plus workers) the system may run.
    static final int MAX_INSTANCES = WorkerOrchestrator.MAX_INSTANCES;


    private final String managerID;
    private final Queue managerQueue;
    private final Queue workerQueue;
    private final Queue doneOcrQueue;

    // manager-clients-n: clients this manager serves at once.
    // worker-files-n: target input files per worker.
    private final int workerFilesN;

    // Worker instance IDs this manager launched; the source of truth for its workers.
    private final WorkerOrchestrator workers;

    private final ClientRegistry clients;
    private final ManagerSeams seams = new ManagerSeams() {
        public String managerID() { return managerID; }

        public Queue workerQueue() { return workerQueue; }

        public ClientRegistry clients() { return clients; }

        public void submit(IoWorker.Job job) { ioWorker.submit(job); }

        public void runPendingFinishes() { Manager.this.runPendingFinishes(); }

        public void updateFreeSlots() { Manager.this.updateFreeSlots(); }

        public List<String> readUrls(String location) throws IOException {
            return Manager.this.readUrls(location);
        }

        public void writeSummary(ClientTask task, String location) throws IOException {
            Manager.this.writeSummary(task, location);
        }

        public void ensureWorkers(int outstanding) { Manager.this.ensureWorkers(outstanding); }

        public void failTask(ClientTask task, String description) {
            Manager.this.failTask(task, description);
        }

        public void logDebug(String message) { Manager.this.logDebug(message); }
    };

    // Runs every blocking job, finishes before fan-outs, on one thread.
    private final IoWorker ioWorker;
    private final ClientFileStore clientFiles = new ClientFileStore();

    private volatile boolean running = true;
    // Set by the first TERMINATE: stop admitting, drain until the registry is empty.
    private final AtomicBoolean terminating = new AtomicBoolean();
    // Last time reconcileWorkersIfDue actually reconciled.
    private volatile long lastReconcileMillis = System.currentTimeMillis();
    // The client that asked to terminate; it gets the ack even if already finished.
    private volatile String terminateRequester;

    // Test helper: fixed manager id, and therefore fixed queue names.
    public Manager(String managerID) { this(managerID, 10, 1); }

    // maxClients is manager-clients-n; tasksPerWorker is worker-files-n.
    public Manager(String managerID, int maxClients, int tasksPerWorker) {
        if (maxClients < 1) {
            throw new IllegalArgumentException("manager-clients-n must be at least 1, got " + maxClients);
        }
        if (tasksPerWorker < 1) {
            throw new IllegalArgumentException("worker-files-n must be at least 1, got " + tasksPerWorker);
        }
        this.managerID = managerID;
        this.ioWorker = new IoWorker("manager-io-worker-" + managerID);
        this.clients = new ClientRegistry(maxClients);
        this.workerFilesN = tasksPerWorker;
        this.managerQueue = Queues.get(Queues.managerQueueName());
        this.workerQueue = Queues.get(Queues.workerQueueName());
        this.doneOcrQueue = Queues.get(Queues.doneOcrQueueName(managerID));
        this.workers = new WorkerOrchestrator(new WorkerOrchestrator.Port() {
            public int liveWorkerCount() { return Manager.this.liveWorkerCount(); }

            public int liveInstanceCount() { return Manager.this.liveInstanceCount(); }

            public List<String> launchWorkerInstances(int count) {
                return Manager.this.launchWorkerInstances(count);
            }

            public void terminateInstances(List<String> ids) { Manager.this.terminateInstances(ids); }

            public Map<String, String> workerStates(Collection<String> ids) {
                return Manager.this.workerStates(ids);
            }

            public void ensureWorkers(int outstanding) { Manager.this.ensureWorkers(outstanding); }

            public int totalOutstanding() { return Manager.this.totalOutstanding(); }

            public boolean isTerminating() { return terminating.get(); }

            public void log(String message) { logDebug(message); }
        }, tasksPerWorker);
    }

    public String getManagerID() { return managerID; }

    public boolean isRunning() { return running; }

    // True while this manager has a free client slot.
    public boolean hasCapacity() { return clients.hasCapacity(); }

    public int getActiveClients() { return clients.size(); }

    public int getServedClients() { return clients.served(); }

    public int getRejectedClients() { return clients.rejected(); }

    // One router tick: drains done-OCR without waiting, then polls requests. A full manager
    // requeues the request on manager_queue; duplicates are ignored. No S3 or EC2 call here.
    public void routerTick() {
        List<Received> results = doneOcrQueue.receive(RECEIVE_MAX, 0);
        if (!results.isEmpty()) {
            logDebug("received " + results.size() + " message(s) on " + Queues.doneOcrQueueName(managerID));
        }
        for (Received received : results) {
            handle(received, doneOcrQueue);
        }
        if (!running) {
            return;
        }
        List<Received> requests = managerQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS);
        if (!requests.isEmpty()) {
            logDebug("received " + requests.size() + " message(s) on " + Queues.managerQueueName());
        }
        for (Received received : requests) {
            handle(received, managerQueue);
        }
    }

    // Test seam: one router tick, every pending IoWorker job inline, then a reconcile.
    public void startOnce() {
        routerTick();
        runPendingIo();
        finishTerminationIfDrained();
        reconcileWorkers();
    }

    // Test seam: runs every pending IoWorker job inline, finishes first.
    public void runPendingIo() { ioWorker.runAll(); }

    // Test seam: runs one pending IoWorker job; false when none is queued.
    public boolean runOnePendingJob() { return ioWorker.runOne(); }

    // Runs queued finishes now; fan-out calls this between batches.
    public void runPendingFinishes() { ioWorker.runPendingFinishes(); }

    // Starts the IoWorker plus the Router loop. args: manager-clients-n, worker-files-n.
    public static void main(String[] args) {
        Queues.setDev(false);
        for (String arg : args) {
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        int managerClientsN;
        int workerFilesN;
        try {
            managerClientsN = parseArg(args, 0, "manager-clients-n", 10);
            workerFilesN = parseArg(args, 1, "worker-files-n", 1);
        } catch (IllegalArgumentException e) {
            System.err.println("[MANAGER] " + e.getMessage());
            System.err.println("[MANAGER] Usage: java -jar Manager.jar [manager-clients-n] [worker-files-n]");
            System.exit(2);
            return;
        }
        System.out.println("[MANAGER] Starting Manager node in AWS SQS mode (managerClientsN="
                + managerClientsN + ", workerFilesN=" + workerFilesN + ")...");
        String instanceId = (args.length > 2 && !args[2].trim().isEmpty()) ? args[2].trim() : UUID.randomUUID().toString();
        Manager manager = new Manager(instanceId, managerClientsN, workerFilesN);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MANAGER] Shutting down...");
            manager.shutdown();
        }));

        manager.run();
        System.out.println("[MANAGER] Manager stopped.");
    }

    // Parses a positive integer, or returns defaultValue when absent.
    static int parseArg(String[] args, int index, String name, int defaultValue) {
        if (args == null || args.length <= index) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(args[index].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got '" + args[index] + "'", e);
        }
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least 1, got " + value);
        }
        return value;
    }

    static int workersNeeded(int urlCount, int workerFilesN) {
        return WorkerOrchestrator.workersNeeded(urlCount, workerFilesN);
    }

    @Override
    public void run() {
        ioWorker.start();
        try {
            while (running) {
                try {
                    routerTick();
                    reconcileWorkersIfDue();
                    finishTerminationIfDrained();
                } catch (Exception e) {
                    // Keep the manager alive: one bad batch must not take the whole node down.
                    System.err.println("[MANAGER] receive loop error: " + e.getMessage());
                }
            }
        } finally {
            // Graceful stop: the IoWorker drains the jobs that are queued, then exits.
            ioWorker.stopAfterDrain();
        }
    }

    public void shutdown() { running = false; }

    private void handle(Received received, Queue source) {
        Message msg = received.getMessage();
        logDebug("handling " + msg.getType() + " client=" + msg.getLocalAppID()
                + (msg.getType() == Message.MessageType.DONE_OCR ? " url=" + msg.getImageURL() : ""));
        try {
            if (msg.getType() == null) {
                logDebug("manager ignoring message with no type");
                source.delete(received);
                return;
            }
            switch (msg.getType()) {
                case NEW_TASK:
                    boolean requeue = false;
                    try {
                        requeue = handleNewTask(msg);
                    } catch (Exception e) {
                        System.err.println("[MANAGER] failed to handle NEW_TASK: " + e.getMessage());
                        Message error = Message.createDoneTask(
                                "Manager error: " + e.getMessage(), msg.getLocalAppID());
                        error.setError(true);
                        try {
                            Queues.get(Queues.clientQueueName(msg.getLocalAppID())).send(error);
                        } catch (Exception sendEx) {
                            System.err.println("[MANAGER] could not send error reply: " + sendEx.getMessage());
                        }
                    }
                    if (requeue) {
                        // A failed release must not fall through to the outer catch: that one
                        // deletes the request and the client would wait forever. Leave the
                        // message undeleted instead, so SQS redelivers it after the visibility
                        // timeout.
                        try {
                            source.release(received, REQUEUE_DELAY_SECONDS);
                        } catch (Exception e) {
                            System.err.println("[MANAGER] could not requeue client "
                                    + msg.getLocalAppID() + "; leaving the request for redelivery: "
                                    + e.getMessage());
                        }
                    } else {
                        source.delete(received);
                    }
                    break;
                case DONE_OCR:
                    handleDoneOcr(msg);
                    source.delete(received);
                    break;
                case TERMINATE:
                    handleTerminate(msg);
                    source.delete(received);
                    break;
                default:
                    logDebug("unknown message type on manager queue: " + msg.getType());
                    source.delete(received);
                    break;
            }
        } catch (Exception e) {
            System.err.println("[MANAGER] unexpected error in handle: " + e.getMessage());
            try { source.delete(received); } catch (Exception ignored) { }
        }
    }

    // Admits a client and submits its fan-out to the IoWorker. No S3 or EC2 call here.
    // Returns true when the request must go back on manager_queue (no free slot): the caller
    // releases it with a delay. Terminating and duplicate requests are handled here, return false.
    private boolean handleNewTask(Message msg) {
        String localAppID = msg.getLocalAppID();
        if (terminating.get()) {
            clients.markRejected();
            logDebug("terminating: rejecting client " + localAppID);
            Message rejected = Message.createDoneTask(null, localAppID);
            rejected.setError(true);
            Queues.get(Queues.clientQueueName(localAppID)).send(rejected);
            return false;
        }
        if (clients.contains(localAppID)) {
            logDebug("ignoring duplicate new task for client " + localAppID);
            return false;
        }
        if (!hasCapacity()) {
            clients.markRejected();
            logDebug("full: requeueing client " + localAppID + " (" + clients.size() + "/" + clients.maxClients() + ")");
            return true;
        }
        ClientTask task = new ClientTask(localAppID, msg.getInputFileLocation());
        clients.add(task);
        ioWorker.submit(new SplitInputJob(seams, task));
        logDebug("admitted client " + localAppID);
        return false;
    }

    // Records a result and queues finish when the last one arrives.
    private void handleDoneOcr(Message msg) {
        ClientTask task = clients.get(msg.getLocalAppID());
        if (task == null) {
            logDebug("ignoring done OCR for unknown client " + msg.getLocalAppID());
            return;
        }
        // First text wins, but count every result: duplicate URLs are separate tasks, and
        // an SQS redelivery cannot be told apart from a second identical-URL task.
        String text = msg.getIdentifiedText() == null ? "" : msg.getIdentifiedText();
        if (msg.isError() && !text.startsWith("ERROR:")) {
            text = "ERROR: " + text;
        }
        task.texts.putIfAbsent(msg.getImageURL(), text);
        int left = task.outstanding.decrementAndGet();
        logDebug("client " + msg.getLocalAppID() + ": " + left + " image(s) outstanding");
        if (left <= 0) {
            ioWorker.submit(new CompleteClientJob(seams, task));
        }
    }

    // Stops admission and records the requester; the drain and ack happen later.
    private void handleTerminate(Message msg) {
        if (!terminating.compareAndSet(false, true)) {
            logDebug("already terminating; ignoring duplicate terminate");
            return;
        }
        terminateRequester = msg.getLocalAppID();
        logDebug("terminating: draining " + clients.size() + " client(s)");
    }

    // Completes termination once every admitted client has its answer: stop this manager's own
    // workers, drop the done-OCR queue, ack the requester (even if it already finished), stop.
    private void finishTerminationIfDrained() {
        if (!running || !terminating.get() || !clients.isEmpty()) {
            return;
        }
        running = false;
        stopWorkers();
        deleteDoneOcrQueueIfAws();
        String requester = terminateRequester;
        if (requester == null) {
            return;
        }
        try {
            Queues.get(Queues.clientQueueName(requester))
                    .send(Message.createTerminate(requester));
        } catch (Exception e) {
            System.err.println("[MANAGER] could not ack terminate: " + e.getMessage());
        }
    }

    protected void ensureWorkers(int outstanding) { workers.ensure(outstanding); }

    // Live workers fleet-wide, all managers included; protected so tests never reach AWS.
    protected int liveWorkerCount() { return AWS.getInstance().runningWorkerCount(); }

    // Live instances fleet-wide (managers plus workers); protected so tests never reach AWS.
    protected int liveInstanceCount() { return AWS.getInstance().runningInstanceCount(); }

    protected List<String> launchWorkers(int count) { return workers.launch(count); }

    // Launches worker instances; protected so tests can supply IDs without reaching AWS.
    protected List<String> launchWorkerInstances(int count) { return workers.launchInstances(count); }

    // User-data script for one worker; package-private so tests can check it without AWS.
    String workerLaunchScript(String bucketName) { return workers.launchScript(bucketName); }


    private int totalOutstanding() { return clients.totalOutstanding(); }

    // Fails a task once: error reply, remove it, free the slot. IoWorker only.
    private void failTask(ClientTask task, String description) {
        if (!task.done.compareAndSet(false, true)) {
            return; // already answered
        }
        System.err.println("[MANAGER] failing client " + task.localAppID + ": " + description);
        try {
            Message failed = Message.createDoneTask(description, task.localAppID);
            failed.setError(true);
            Queues.get(Queues.clientQueueName(task.localAppID)).send(failed);
        } catch (Exception e) {
            System.err.println("[MANAGER] could not notify client " + task.localAppID
                    + " of failure: " + e.getMessage());
        }
        clients.remove(task);
        updateFreeSlots();
    }

    // Publishes free slots as the EC2 free tag; a hint that may lag. IoWorker only.
    private void updateFreeSlots() { publishFreeSlots(clients.freeSlots()); }

    // Protected seam for the free-slot tag write; no-op in dev so tests never reach AWS.
    protected void publishFreeSlots(int free) {
        if (Queues.isDev()) {
            return;
        }
        try {
            AWS.getInstance().setFreeSlots(managerID, free);
        } catch (Exception e) {
            System.err.println("[MANAGER] could not update free slots tag: " + e.getMessage());
        }
    }


    protected void writeSummary(ClientTask task, String summaryLocation) throws IOException {
        clientFiles.writeSummary(task.urls, task.texts, summaryLocation);
    }

    protected List<String> readUrls(String inputFileLocation) throws IOException {
        return clientFiles.readUrls(inputFileLocation);
    }

    private void stopWorkers() { workers.stop(); }

    // Requests termination through AWS; protected so termination can be tested offline.
    protected void terminateInstances(List<String> instanceIds) {
        AWS.getInstance().terminateInstances(instanceIds);
    }

    // Current EC2 state per worker ID; protected so reconcile can be tested offline.
    protected Map<String, String> workerStates(Collection<String> instanceIds) {
        return AWS.getInstance().instanceStates(instanceIds);
    }

    protected void reconcileWorkers() { workers.reconcile(); }

    // Reconciles at most once per WORKER_RECONCILE_INTERVAL_MILLIS.
    private void reconcileWorkersIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastReconcileMillis < WORKER_RECONCILE_INTERVAL_MILLIS) {
            return;
        }
        lastReconcileMillis = now;
        reconcileWorkers();
    }

    // Test seam: the IDs of the workers this manager launched.
    protected Set<String> getWorkerInstanceIds() { return workers.ids(); }

    // Deletes the per-manager done-OCR queue in AWS mode; a no-op in dev.
    protected void deleteDoneOcrQueueIfAws() {
        if (Queues.isDev()) {
            return;
        }
        try {
            AWS aws = AWS.getInstance();
            aws.deleteQueue(aws.createQueue(Queues.doneOcrQueueName(managerID)));
        } catch (Exception e) {
            System.err.println("[MANAGER] could not delete done OCR queue: " + e.getMessage());
        }
    }

    static String describe(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    private void logDebug(String message) {
        if (debug || client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }
}
