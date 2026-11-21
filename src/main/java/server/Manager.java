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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Routes client work to workers and answers each client once its images are OCRed.
 *
 * Two threads: the main Router receives and admits; one IoWorker does all blocking calls.
 * The manager never OCRs or downloads images.
 */
public class Manager implements Runnable {
    private static final int RECEIVE_MAX = 10;
    private static final int RECEIVE_WAIT_SECONDS = 1;
    /** NEW_IMAGE_TASK messages are sent in batches of this size. */
    private static final int FAN_OUT_BATCH = 10;

    /** Maximum EC2 instances (manager plus workers) the system may run. */
    static final int MAX_INSTANCES = 19;

    private static final String WORKER_TAG = "worker";
    private static final String WORKER_JAR_KEY = AWS.S3_PREFIX + "Worker.jar";
    private static final String WORKER_JAR_PATH = "/home/ec2-user/Worker.jar";

    private final String managerID;
    private final Queue managerQueue;
    private final Queue workerQueue;
    private final Queue doneOcrQueue;

    /** manager-clients-n: maximum clients this manager serves at once. */
    private final int maxClients;
    /** worker-files-n: target number of input files per worker. */
    private final int workerFilesN;

    /** Admitted clients keyed by localAppID; its size is the load. */
    private final Map<String, ClientTask> clientTasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger servedClients = new AtomicInteger(); // finished clients
    private final AtomicInteger rejectedClients = new AtomicInteger(); // busy replies

    /** Runs every blocking job, finishes before fan-outs, on one thread. */
    private final IoWorker ioWorker;

    private volatile boolean running = true;

    /** Test helper: fixed manager id, and therefore fixed queue names. */
    public Manager(String managerID) {
        this(managerID, 10, 1);
    }

    /** maxClients is manager-clients-n; tasksPerWorker is worker-files-n. */
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

    /** True while this manager has a free client slot (fewer than maxClients admitted). */
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

    /**
     * One router tick. Drains done-OCR without waiting, then polls requests. A full manager
     * answers busy; duplicates are ignored. No S3 or EC2 call here.
     */
    public void routerTick() {
        for (Received received : doneOcrQueue.receive(RECEIVE_MAX, 0)) {
            handle(received, doneOcrQueue);
        }
        if (!running) {
            return;
        }
        for (Received received : managerQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS)) {
            handle(received, managerQueue);
        }
    }

    /** Test seam: one router tick, then every pending IoWorker job inline. */
    public void startOnce() {
        routerTick();
        runPendingIo();
    }

    /** Test seam: runs every pending IoWorker job inline, finishes first. */
    public void runPendingIo() {
        ioWorker.runAll();
    }

    /** Test seam: runs one pending IoWorker job; false when none is queued. */
    public boolean runOnePendingJob() {
        return ioWorker.runOne();
    }

    /** Runs queued finishes now; fan-out calls this between batches. */
    public void runPendingFinishes() {
        ioWorker.runPendingFinishes();
    }

    /** Starts the IoWorker plus the Router loop. args: manager-clients-n, worker-files-n. */
    public static void main(String[] args) {
        Queues.setDev(false);
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
        String instanceId = args.length > 2 ? args[2] : UUID.randomUUID().toString();
        Manager manager = new Manager(instanceId, managerClientsN, workerFilesN);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MANAGER] Shutting down...");
            manager.shutdown();
        }));

        manager.run();
        System.out.println("[MANAGER] Manager stopped.");
    }

    /** Parses a positive integer, or returns defaultValue when absent. */
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

    /** ceil(urlCount / workerFilesN); zero when there are no URLs. */
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
        try {
            if (msg.getType() == null) {
                logDebug("manager ignoring message with no type");
                source.delete(received);
                return;
            }
            switch (msg.getType()) {
                case NEW_TASK:
                    try {
                        handleNewTask(msg);
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
                    source.delete(received);
                    break;
                case DONE_OCR:
                    handleDoneOcr(msg);
                    source.delete(received);
                    break;
                case TERMINATE:
                    handleTerminate();
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

    /** Admits a client and submits its fan-out to the IoWorker. No S3 or EC2 call here. */
    private void handleNewTask(Message msg) {
        String localAppID = msg.getLocalAppID();
        if (clientTasks.containsKey(localAppID)) {
            logDebug("ignoring duplicate new task for client " + localAppID);
            return;
        }
        if (!hasCapacity()) {
            rejectedClients.incrementAndGet();
            logDebug("busy: rejecting client " + localAppID + " (" + clientTasks.size() + "/" + maxClients + ")");
            Message busy = Message.createDoneTask(null, localAppID);
            busy.setError(true);
            Queues.get(Queues.clientQueueName(localAppID)).send(busy);
            return;
        }
        ClientTask task = new ClientTask(localAppID, msg.getInputFileLocation());
        clientTasks.put(localAppID, task);
        ioWorker.submit(new FanOutJob(task));
        logDebug("admitted client " + localAppID);
    }

    /** Records a result and queues finish when the last one arrives. */
    private void handleDoneOcr(Message msg) {
        ClientTask task = clientTasks.get(msg.getLocalAppID());
        if (task == null) {
            logDebug("ignoring done OCR for unknown client " + msg.getLocalAppID());
            return;
        }
        // First text wins, but count every result: duplicate URLs are separate tasks, and
        // an SQS redelivery cannot be told apart from a second identical-URL task.
        task.texts.putIfAbsent(msg.getImageURL(), msg.getIdentifiedText());
        if (task.outstanding.decrementAndGet() <= 0) {
            ioWorker.submit(new FinishJob(task));
        }
    }

    private void handleTerminate() {
        running = false;
        for (ClientTask task : clientTasks.values()) {
            if (!task.done.get()) {
                logDebug("terminating client " + task.localAppID + " with " + task.outstanding.get() + " tasks left");
            }
            Queues.get(Queues.clientQueueName(task.localAppID))
                    .send(Message.createTerminate(task.localAppID));
        }
        deleteDoneOcrQueueIfAws();
    }

    /** Starts the workers needed for the outstanding images, capped by MAX_INSTANCES. */
    private void ensureWorkers(int outstanding) {
        if (Queues.isDev() || outstanding <= 0) {
            return;
        }
        try {
            AWS aws = AWS.getInstance();
            int liveWorkers = aws.runningWorkerCount();
            int liveInstances = aws.runningInstanceCount();
            int needed = workersNeeded(outstanding, workerFilesN);
            int toStart = needed - liveWorkers;
            if (toStart <= 0) {
                return;
            }
            int allowed = MAX_INSTANCES - liveInstances;
            if (toStart > allowed) {
                toStart = allowed;
            }
            if (toStart <= 0) {
                logDebug("instance cap reached (" + liveInstances + "/" + MAX_INSTANCES
                        + " live, " + liveWorkers + " workers)");
                return;
            }
            launchWorkers(toStart);
            logDebug("launched " + toStart + " worker(s): " + outstanding + " image(s), "
                    + workerFilesN + " per worker, " + liveWorkers + " already live");
        } catch (Exception e) {
            // Worker bootstrap is best effort: the manager must keep serving clients.
            System.err.println("[MANAGER] could not launch workers: " + e.getMessage());
        }
    }

    /** Starts worker instances that download the uploaded worker jar and run the worker. */
    private void launchWorkers(int count) {
        AWS aws = AWS.getInstance();
        String script = "#!/bin/bash\n"
                + "aws s3 cp s3://" + aws.bucketName + "/" + WORKER_JAR_KEY + " " + WORKER_JAR_PATH + "\n"
                + "JAVA_BIN=$(ls /usr/lib/jvm/*1.8*/bin/java 2>/dev/null | head -n 1)\n"
                + "[ -z \"$JAVA_BIN\" ] && JAVA_BIN=$(command -v java)\n"
                + "\"$JAVA_BIN\" -jar " + WORKER_JAR_PATH + " > /home/ec2-user/worker.log 2>&1 || { cat /home/ec2-user/worker.log > /dev/console; exit 1; }\n";
        aws.createEC2(script, WORKER_TAG, count);
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

    /** Fails a task once: error reply, remove it, free the slot. IoWorker only. */
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

    /** Publishes free slots as the EC2 free tag; a hint that may lag. IoWorker only. */
    private void updateFreeSlots() {
        if (Queues.isDev()) {
            return;
        }
        try {
            AWS.getInstance().setFreeSlots(managerID, maxClients - clientTasks.size());
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

    /** Deletes the per-manager done-OCR queue in AWS mode; a no-op in dev. */
    private void deleteDoneOcrQueueIfAws() {
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
        if (client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    /** Reads the input, sends image tasks in batches, drains finishes between batches. */
    private final class FanOutJob extends IoWorker.Job {
        private final ClientTask task;

        FanOutJob(ClientTask task) {
            super(IoWorker.FAN_OUT);
            this.task = task;
        }

        @Override
        public void run() {
            try {
                List<String> urls = Collections.unmodifiableList(readUrls(task.inputFileLocation));
                task.urls = urls;
                task.outstanding.set(urls.size());
                List<Message> batch = new ArrayList<Message>(FAN_OUT_BATCH);
                for (String url : urls) {
                    batch.add(Message.createNewImageTask(url, task.localAppID, managerID));
                    if (batch.size() == FAN_OUT_BATCH) {
                        workerQueue.sendAll(batch);
                        batch = new ArrayList<Message>(FAN_OUT_BATCH);
                        runPendingFinishes();
                    }
                }
                if (!batch.isEmpty()) {
                    workerQueue.sendAll(batch);
                }
                logDebug("admitted client " + task.localAppID + " with " + urls.size() + " images");
                if (urls.isEmpty()) {
                    ioWorker.submit(new FinishJob(task)); // empty input finishes at once
                }
                ensureWorkers(totalOutstanding());
            } catch (Exception e) {
                failTask(task, "Input failure: " + describe(e));
            }
        }
    }

    /** Answers once: summary, DONE_TASK, remove the task, free the slot. */
    private final class FinishJob extends IoWorker.Job {
        private final ClientTask task;

        FinishJob(ClientTask task) {
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

    /** Per-client work while its images are OCRed. */
    static final class ClientTask {
        final String localAppID;
        final String inputFileLocation;
        /** First text per URL wins. */
        final ConcurrentHashMap<String, String> texts = new ConcurrentHashMap<String, String>();
        /** Image tasks not yet answered. */
        final AtomicInteger outstanding = new AtomicInteger();
        /** Answers the client exactly once. */
        final AtomicBoolean done = new AtomicBoolean(false);
        /** Input order, set before the first image task is sent. */
        volatile List<String> urls = Collections.emptyList();

        ClientTask(String localAppID, String inputFileLocation) {
            this.localAppID = localAppID;
            this.inputFileLocation = inputFileLocation;
        }
    }
}
