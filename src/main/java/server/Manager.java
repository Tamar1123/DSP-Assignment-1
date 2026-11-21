package server;
import utils.AWS;
import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    // NEW_IMAGE_TASK batch size.
    private static final int IMAGE_TASK_BATCH = 10;
    // How often run() reconciles the worker set and launches crash replacements.
    private static final long WORKER_RECONCILE_INTERVAL_MILLIS = 10000L;

    // Max EC2 instances (managers plus workers) the system may run.
    static final int MAX_INSTANCES = 9;

    private static final String WORKER_TAG = "worker";
    private static final String WORKER_JAR_KEY = AWS.S3_PREFIX + "Worker.jar";
    private static final String WORKER_JAR_PATH = "/home/ec2-user/Worker.jar";

    private final String managerID;
    private final Queue managerQueue;
    private final Queue workerQueue;
    private final Queue doneOcrQueue;

    // manager-clients-n: clients this manager serves at once.
    private final int maxClients;
    // worker-files-n: target input files per worker.
    private final int workerFilesN;

    // Worker instance IDs this manager launched; the source of truth for its workers.
    private final Set<String> workerInstanceIds = ConcurrentHashMap.newKeySet();

    // Admitted clients keyed by localAppID; its size is the load.
    private final Map<String, ClientTask> clientTasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger servedClients = new AtomicInteger(); // finished clients
    private final AtomicInteger rejectedClients = new AtomicInteger(); // full-manager requeues

    // Runs every blocking job, finishes before fan-outs, on one thread.
    private final IoWorker ioWorker;

    private volatile boolean running = true;
    // Set by the first TERMINATE: stop admitting, drain until clientTasks is empty.
    private final AtomicBoolean terminating = new AtomicBoolean();
    // Last time reconcileWorkersIfDue actually reconciled.
    private volatile long lastReconcileMillis = System.currentTimeMillis();
    // The client that asked to terminate; it gets the ack even if already finished.
    private volatile String terminateRequester;

    // Test helper: fixed manager id, and therefore fixed queue names.
    public Manager(String managerID) {
        this(managerID, 10, 1);
    }

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
        this.maxClients = maxClients;
        this.workerFilesN = tasksPerWorker;
        this.managerQueue = Queues.get(Queues.managerQueueName());
        this.workerQueue = Queues.get(Queues.workerQueueName());
        this.doneOcrQueue = Queues.get(Queues.doneOcrQueueName(managerID));
    }

    public String getManagerID() {
        return managerID;
    }

    public boolean isRunning() {
        return running;
    }

    // True while this manager has a free client slot.
    public boolean hasCapacity() {
        return clientTasks.size() < maxClients;
    }

    public int getActiveClients() {
        return clientTasks.size();
    }

    public int getServedClients() {
        return servedClients.get();
    }

    public int getRejectedClients() {
        return rejectedClients.get();
    }

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
    public void runPendingIo() {
        ioWorker.runAll();
    }

    // Test seam: runs one pending IoWorker job; false when none is queued.
    public boolean runOnePendingJob() {
        return ioWorker.runOne();
    }

    // Runs queued finishes now; fan-out calls this between batches.
    public void runPendingFinishes() {
        ioWorker.runPendingFinishes();
    }

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

    // ceil(urlCount / workerFilesN); zero with no URLs.
    static int workersNeeded(int urlCount, int workerFilesN) {
        if (workerFilesN < 1) {
            throw new IllegalArgumentException("worker-files-n must be at least 1, got " + workerFilesN);
        }
        if (urlCount <= 0) {
            return 0;
        }
        return (urlCount - 1) / workerFilesN + 1;
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

    public void shutdown() {
        running = false;
    }

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
            rejectedClients.incrementAndGet();
            logDebug("terminating: rejecting client " + localAppID);
            Message rejected = Message.createDoneTask(null, localAppID);
            rejected.setError(true);
            Queues.get(Queues.clientQueueName(localAppID)).send(rejected);
            return false;
        }
        if (clientTasks.containsKey(localAppID)) {
            logDebug("ignoring duplicate new task for client " + localAppID);
            return false;
        }
        if (!hasCapacity()) {
            rejectedClients.incrementAndGet();
            logDebug("full: requeueing client " + localAppID + " (" + clientTasks.size() + "/" + maxClients + ")");
            return true;
        }
        ClientTask task = new ClientTask(localAppID, msg.getInputFileLocation());
        clientTasks.put(localAppID, task);
        ioWorker.submit(new SplitInputJob(task));
        logDebug("admitted client " + localAppID);
        return false;
    }

    // Records a result and queues finish when the last one arrives.
    private void handleDoneOcr(Message msg) {
        ClientTask task = clientTasks.get(msg.getLocalAppID());
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
            ioWorker.submit(new CompleteClientJob(task));
        }
    }

    // Stops admission and records the requester; the drain and ack happen later.
    private void handleTerminate(Message msg) {
        if (!terminating.compareAndSet(false, true)) {
            logDebug("already terminating; ignoring duplicate terminate");
            return;
        }
        terminateRequester = msg.getLocalAppID();
        logDebug("terminating: draining " + clientTasks.size() + " client(s)");
    }

    // Completes termination once every admitted client has its answer: stop this manager's own
    // workers, drop the done-OCR queue, ack the requester (even if it already finished), stop.
    private void finishTerminationIfDrained() {
        if (!running || !terminating.get() || !clientTasks.isEmpty()) {
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

    // Starts the workers needed for the outstanding images, sized against the whole fleet and
    // capped by the global instance limit.
    protected void ensureWorkers(int outstanding) {
        if (Queues.isDev() || outstanding <= 0) {
            return;
        }
        try {
            int needed = workersNeeded(outstanding, workerFilesN);
            int liveWorkers = liveWorkerCount();
            int liveInstances = liveInstanceCount();
            int toStart = Math.min(needed - liveWorkers, MAX_INSTANCES - liveInstances);
            if (toStart <= 0) {
                logDebug("no workers to launch: " + needed + " needed, " + liveWorkers
                        + " live fleet-wide, instance cap at " + liveInstances + "/" + MAX_INSTANCES);
                return;
            }
            List<String> launched = launchWorkers(toStart);
            logDebug("launched " + launched.size() + " worker(s): " + outstanding + " image(s), "
                    + workerFilesN + " per worker, " + liveWorkers + " were live fleet-wide, "
                    + liveInstances + "/" + MAX_INSTANCES + " instances before launch");
        } catch (Exception e) {
            // Worker bootstrap is best effort: the manager must keep serving clients.
            System.err.println("[MANAGER] could not launch workers: " + e.getMessage());
        }
    }

    // Live workers fleet-wide, all managers included; protected so tests never reach AWS.
    protected int liveWorkerCount() {
        return AWS.getInstance().runningWorkerCount();
    }

    // Live instances fleet-wide (managers plus workers); protected so tests never reach AWS.
    protected int liveInstanceCount() {
        return AWS.getInstance().runningInstanceCount();
    }

    // Launches workers and records their IDs as this manager's workers.
    protected List<String> launchWorkers(int count) {
        List<String> launched = launchWorkerInstances(count);
        workerInstanceIds.addAll(launched);
        return launched;
    }

    // Launches worker instances; protected so tests can supply IDs without reaching AWS.
    protected List<String> launchWorkerInstances(int count) {
        AWS aws = AWS.getInstance();
        return aws.createEC2Instances(workerLaunchScript(aws.bucketName), WORKER_TAG, count);
    }

    // User-data script for one worker; package-private so tests can check it without AWS.
    String workerLaunchScript(String bucketName) {
        return "#!/bin/bash\n"
                + "set -e\n"
                + "exec > >(tee -a /var/log/dsp-bootstrap.log) 2>&1\n"
                + "mkdir -p /home/ec2-user\n"
                + "export DEBIAN_FRONTEND=noninteractive\n"
                // No OCR install: Tesseract, Leptonica and the trained data ship inside Worker.jar.
                + "if ! command -v aws >/dev/null 2>&1 || ! command -v java >/dev/null 2>&1; then\n"
                + "  apt-get update -y && apt-get install -y awscli openjdk-11-jre-headless\n"
                + "fi\n"
                + "aws s3 cp s3://" + bucketName + "/" + WORKER_JAR_KEY + " " + WORKER_JAR_PATH + "\n"
                + "JAVA_BIN=$(command -v java)\n"
                + (debug ? "touch /home/ec2-user/worker.log\n(while true; do aws s3 cp /home/ec2-user/worker.log s3://"
                + bucketName + "/" + AWS.S3_PREFIX + "logs/worker-$(hostname).log --quiet; sleep 5; done) &\n" : "")
                // A worker exit (normal or crash) terminates its own instance so the manager sees it die.
                + "set +e\n"
                + "\"$JAVA_BIN\" -jar " + WORKER_JAR_PATH + " > /home/ec2-user/worker.log 2>&1\n"
                + "STATUS=$?\n"
                + "cat /home/ec2-user/worker.log > /dev/console || true\n"
                + "TOKEN=$(curl -s -X PUT \"http://169.254.169.254/latest/api/token\" "
                + "-H \"X-aws-ec2-metadata-token-ttl-seconds: 21600\")\n"
                + "INSTANCE_ID=$(curl -s -H \"X-aws-ec2-metadata-token: $TOKEN\" "
                + "http://169.254.169.254/latest/meta-data/instance-id)\n"
                + "aws ec2 terminate-instances --region " + AWS.region2.id()
                + " --instance-ids \"$INSTANCE_ID\" || true\n"
                + "exit $STATUS\n";
    }

    private int totalOutstanding() {
        int total = 0;
        for (ClientTask task : clientTasks.values()) {
            if (task.outstanding.get() > 0) {
                total += task.outstanding.get();
            }
        }
        return total;
    }

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
        clientTasks.remove(task.localAppID, task);
        updateFreeSlots();
    }

    // Publishes free slots as the EC2 free tag; a hint that may lag. IoWorker only.
    private void updateFreeSlots() {
        publishFreeSlots(maxClients - clientTasks.size());
    }

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
        // In dev mode summaryLocation is a local path; on AWS it is an S3 key, so go via a temp file.
        File file = Queues.isDev() ? new File(summaryLocation) : File.createTempFile("manager-summary-", ".txt");
        PrintWriter out = new PrintWriter(file);
        try {
            // Input URL order (task.urls), never the map's order.
            for (String url : task.urls) {
                String text = task.texts.get(url);
                text = (text == null ? "" : text).replace('\n', ' ').replace('\r', ' ');
                out.println(url + "\t" + text);
            }
        } finally {
            out.close();
        }
        if (!Queues.isDev()) {
            String s3Key = summaryLocation;
            AWS.getInstance().getS3().putObject(
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket(AWS.getInstance().bucketName)
                            .key(s3Key)
                            .build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromFile(file));
            file.delete();
        }
    }

    protected List<String> readUrls(String inputFileLocation) throws IOException {
        if (!Queues.isDev()) {
            // Download from S3 to a temp file, then parse.
            File tempFile = File.createTempFile("manager-input-", ".txt");
            tempFile.deleteOnExit();
            tempFile.delete(); // the SDK refuses to overwrite an existing file
            AWS.getInstance().getS3().getObject(
                    software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                            .bucket(AWS.getInstance().bucketName)
                            .key(inputFileLocation)
                            .build(),
                    software.amazon.awssdk.core.sync.ResponseTransformer.toFile(tempFile));
            return readUrlsFromFile(tempFile);
        }
        return readUrlsFromFile(new File(inputFileLocation));
    }

    private List<String> readUrlsFromFile(File file) throws IOException {
        List<String> urls = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                String url = line.trim();
                if (!url.isEmpty()) {
                    urls.add(url);
                }
            }
        } finally {
            reader.close();
        }
        return urls;
    }

    // Terminates only the workers this manager launched; never calls AWS in dev mode.
    private void stopWorkers() {
        if (Queues.isDev()) {
            return;
        }
        List<String> ids = new ArrayList<String>(workerInstanceIds);
        if (ids.isEmpty()) {
            return;
        }
        try {
            terminateInstances(ids);
        } catch (Exception e) {
            // Worker shutdown is best effort and must not prevent the terminate acknowledgement.
            System.err.println("[MANAGER] could not stop workers: " + e.getMessage());
        } finally {
            workerInstanceIds.clear();
        }
    }

    // Requests termination through AWS; protected so termination can be tested offline.
    protected void terminateInstances(List<String> instanceIds) {
        AWS.getInstance().terminateInstances(instanceIds);
    }

    // Current EC2 state per worker ID; protected so reconcile can be tested offline.
    protected Map<String, String> workerStates(Collection<String> instanceIds) {
        return AWS.getInstance().instanceStates(instanceIds);
    }

    // Drops workers that are gone or stopped and launches replacements for outstanding work.
    // Runs at most every WORKER_RECONCILE_INTERVAL_MILLIS; never while terminating.
    protected void reconcileWorkers() {
        if (terminating.get() || Queues.isDev()) {
            return;
        }
        try {
            if (!workerInstanceIds.isEmpty()) {
                Map<String, String> states = workerStates(workerInstanceIds);
                for (String id : new ArrayList<String>(workerInstanceIds)) {
                    String state = states.get(id);
                    if (state == null || isDeadWorkerState(state)) {
                        workerInstanceIds.remove(id);
                        logDebug("removed dead worker " + id + " (state " + state + ")");
                    }
                }
            }
            ensureWorkers(totalOutstanding());
        } catch (Exception e) {
            // Replacement is best effort: a failed EC2 lookup must not stop the manager.
            System.err.println("[MANAGER] could not reconcile workers: " + e.getMessage());
        }
    }

    // EC2 states in which a worker can no longer process messages. A worker in "shutting-down"
    // may be a cap kill, not a crash, and still counts as live until EC2 removes it.
    private static boolean isDeadWorkerState(String state) {
        return state.equals("terminated")
                || state.equals("stopped");
    }

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
    protected Set<String> getWorkerInstanceIds() {
        return workerInstanceIds;
    }

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

    private static String describe(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    private void logDebug(String message) {
        if (debug || client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    // Reads the input, sends image tasks in batches, drains finishes between batches.
    private final class SplitInputJob extends IoWorker.Job {
        private final ClientTask task;

        SplitInputJob(ClientTask task) {
            super(IoWorker.DISPATCH);
            this.task = task;
        }

        @Override
        public void run() {
            try {
                updateFreeSlots(); // advertise capacity at admission, before the URLs fan out
                List<String> urls = Collections.unmodifiableList(readUrls(task.inputFileLocation));
                task.urls = urls;
                task.outstanding.set(urls.size());
                List<Message> batch = new ArrayList<Message>(IMAGE_TASK_BATCH);
                for (String url : urls) {
                    batch.add(Message.createNewImageTask(url, task.localAppID, managerID));
                    if (batch.size() == IMAGE_TASK_BATCH) {
                        logDebug("sending " + batch.size() + " NEW_IMAGE_TASK to " + Queues.workerQueueName());
                        workerQueue.sendAll(batch);
                        batch = new ArrayList<Message>(IMAGE_TASK_BATCH);
                        runPendingFinishes();
                    }
                }
                if (!batch.isEmpty()) {
                    logDebug("sending " + batch.size() + " NEW_IMAGE_TASK to " + Queues.workerQueueName());
                    workerQueue.sendAll(batch);
                }
                logDebug("admitted client " + task.localAppID + " with " + urls.size() + " images");
                if (urls.isEmpty()) {
                    ioWorker.submit(new CompleteClientJob(task)); // empty input finishes at once
                }
                ensureWorkers(totalOutstanding());
            } catch (Exception e) {
                failTask(task, "Input failure: " + describe(e));
            }
        }
    }

    // Answers once: summary, DONE_TASK, remove the task, free the slot.
    private final class CompleteClientJob extends IoWorker.Job {
        private final ClientTask task;

        CompleteClientJob(ClientTask task) {
            super(IoWorker.FINISH);
            this.task = task;
        }

        @Override
        public void run() {
            if (!task.done.compareAndSet(false, true)) {
                return; // answered once
            }
            String summaryLocation = Queues.isDev()
                    ? task.inputFileLocation + ".summary"
                    : AWS.S3_PREFIX + "summaries/" + task.localAppID + ".txt";
            try {
                writeSummary(task, summaryLocation);
                logDebug("summary written at " + summaryLocation + "; sending DONE_TASK to "
                        + Queues.clientQueueName(task.localAppID));
                Queues.get(Queues.clientQueueName(task.localAppID))
                        .send(Message.createDoneTask(summaryLocation, task.localAppID));
                servedClients.incrementAndGet();
                logDebug("finished client " + task.localAppID + " (served " + servedClients.get()
                        + ", free slots " + (maxClients - clientTasks.size()) + ")");
            } catch (Exception e) {
                // The summary could not be written or sent: tell the client so it does not hang.
                System.err.println("[MANAGER] could not finish client " + task.localAppID
                        + ": " + describe(e));
                try {
                    Message failed = Message.createDoneTask("Summary failure: " + describe(e),
                            task.localAppID);
                    failed.setError(true);
                    Queues.get(Queues.clientQueueName(task.localAppID)).send(failed);
                } catch (Exception sendFailure) {
                    System.err.println("[MANAGER] could not notify client " + task.localAppID
                            + " of failure: " + sendFailure.getMessage());
                }
            } finally {
                // Free the slot last, and always: a failed summary must not leak a client slot.
                clientTasks.remove(task.localAppID, task);
                updateFreeSlots();
            }
        }
    }

    // Per-client work while its images are OCRed.
    static final class ClientTask {
        final String localAppID;
        final String inputFileLocation;
        // First text per URL wins.
        final ConcurrentHashMap<String, String> texts = new ConcurrentHashMap<String, String>();
        // Image tasks not yet answered.
        final AtomicInteger outstanding = new AtomicInteger();
        // Answers the client exactly once.
        final AtomicBoolean done = new AtomicBoolean(false);
        // Input order, set before the first image task is sent.
        volatile List<String> urls = Collections.emptyList();

        ClientTask(String localAppID, String inputFileLocation) {
            this.localAppID = localAppID;
            this.inputFileLocation = inputFileLocation;
        }
    }
}
