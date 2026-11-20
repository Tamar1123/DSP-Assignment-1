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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The manager: routes client work to the shared worker queue and answers each client once
 * all of its images have been OCRed.
 *
 * The manager never OCRs and never downloads images. It owns one request queue (shared by
 * all clients), one shared worker queue, and one {@code done_ocr_<managerID>} queue that its
 * workers report to.
 */
public class Manager implements Runnable {
    private static final int RECEIVE_MAX = 10;
    private static final int RECEIVE_WAIT_SECONDS = 1;

    /** Maximum EC2 instances (manager plus workers) the system may run. */
    static final int MAX_INSTANCES = 19;

    private static final String WORKER_TAG = "worker";
    private static final String WORKER_JAR_KEY = AWS.S3_PREFIX + "Worker.jar";
    private static final String WORKER_JAR_PATH = "/home/ec2-user/" + WORKER_JAR_KEY;

    private final String managerID;
    private final Queue managerQueue;
    private final Queue workerQueue;
    private final Queue doneOcrQueue;

    /** manager-clients-n: maximum clients this manager serves at once. */
    private final int maxClients;
    /** worker-files-n: target number of input files per worker. */
    private final int workerFilesN;

    /** One entry per admitted client (one input file each), keyed by localAppID. Its size is the load. */
    private final Map<String, ClientTask> clientTasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger servedClients = new AtomicInteger(); //cumulative, incremented at finish.
    private final AtomicInteger rejectedClients = new AtomicInteger(); //cumulative, busy replies sent.

    private volatile boolean running = true;

    /** Test-only: gives a test a deterministic manager id, and therefore queue names. */
    public Manager(String managerID) {
        this(managerID, 10, 1);
    }

    /**
     * @param maxClients   manager-clients-n, the maximum number of clients this manager admits
     * @param tasksPerWorker worker-files-n, the target number of input files per worker
     */
    public Manager(String managerID, int maxClients, int tasksPerWorker) {
        if (maxClients < 1) {
            throw new IllegalArgumentException("manager-clients-n must be at least 1, got " + maxClients);
        }
        if (tasksPerWorker < 1) {
            throw new IllegalArgumentException("worker-files-n must be at least 1, got " + tasksPerWorker);
        }
        this.managerID = managerID;
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
     * Processes one receive batch from the request queue and one from the done-OCR queue.
     * This is the test seam; {@link #run()} calls it in a loop until {@link #shutdown()} or a
     * terminate message stops the manager.
     *
     * Admission control: the request queue is polled only while this manager has a free slot.
     * The done-OCR queue is always polled, otherwise a full manager could never finish a client.
     */
    public void startOnce() {
        if (hasCapacity()) {
            for (Received received : managerQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS)) {
                handle(received, managerQueue);
            }
        }
        for (Received received : doneOcrQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS)) {
            handle(received, doneOcrQueue);
        }
    }


    /**
     * Starts the manager. The launch arguments are the two values the client knows:
     * {@code args[0] = manager-clients-n}, {@code args[1] = worker-files-n}.
     */
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

    /** Parses a positive integer argument, falling back to {@code defaultValue} when absent. */
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

    /** workers needed for {@code urlCount} images at {@code workerFilesN} (that many per worker. */
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
        while (running) {
            try {
                startOnce();
            } catch (Exception e) {
                // Keep the manager alive: one bad batch must not take the whole node down.
                System.err.println("[MANAGER] receive loop error: " + e.getMessage());
            }
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

    /**
     * Admits one client and splits its input file into one image task per non-empty line.
     * A duplicate for an admitted client is ignored; a request that arrives when every slot is
     * taken (the race after the poll gate) is answered busy and changes no state.
     */
    private void handleNewTask(Message msg) throws IOException {
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
        List<String> urls = readUrls(msg.getInputFileLocation());
        ClientTask task = new ClientTask(localAppID, msg.getInputFileLocation());
        clientTasks.put(localAppID, task);
        updateFreeSlots();
        try {
            for (String url : urls) {
                workerQueue.send(Message.createNewImageTask(url, localAppID, managerID));
                task.outstanding++;
            }
        } catch (RuntimeException e) {
            clientTasks.remove(localAppID);
            updateFreeSlots();
            throw e;
        }
        ensureWorkers(totalOutstanding());
        logDebug("admitted client " + localAppID + " with " + task.outstanding + " images");
        if (task.outstanding == 0) {
            finish(task);
        }
    }

    /**
     * Boots the missing workers for the images currently outstanding across this manager's
     * clients: {@code ceil(outstanding / worker-files-n)} workers should exist, so start the
     * difference above the live workers. The live worker and total instance counts are read
     * from EC2, and the launch is capped so the whole system never exceeds 20 instances
     * (the manager included). A no-op under the in-memory transport.
     */
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
                + "java -jar " + WORKER_JAR_PATH + " > /home/ec2-user/worker.log 2>&1\n";
        aws.createEC2(script, WORKER_TAG, count);
    }

    private int totalOutstanding() {
        int total = 0;
        for (ClientTask task : clientTasks.values()) {
            if (task.outstanding > 0) {
                total += task.outstanding;
            }
        }
        return total;
    }

    private void handleDoneOcr(Message msg) throws IOException {
        ClientTask task = clientTasks.get(msg.getLocalAppID());
        if (task == null) {
            logDebug("ignoring done OCR for unknown client " + msg.getLocalAppID());
            return;
        }
        task.texts.put(msg.getImageURL(), msg.getIdentifiedText());
        task.outstanding--;
        if (task.outstanding <= 0) {
            finish(task);
        }
    }

    private void handleTerminate() {
        running = false;
        for (ClientTask task : clientTasks.values()) {
            if (!task.done) {
                logDebug("terminating client " + task.localAppID + " with " + task.outstanding + " tasks left");
            }
            Queues.get(Queues.clientQueueName(task.localAppID))
                    .send(Message.createTerminate(task.localAppID));
        }
        deleteDoneOcrQueueIfAws();
    }

    /**
     * Answers the client once, with the location of the summary, and frees its slot. This is a
     * stub for the S3 upload: the summary is written next to the input file and its local path
     * is reported.
     */
    private void finish(ClientTask task) {
        if (task.done) {
            return;
        }
        task.done = true;
        String summaryLocation;
        if (!Queues.isDev()) {
            summaryLocation = AWS.S3_PREFIX + "summaries/" + task.localAppID + ".txt";
        } else {
            summaryLocation = task.inputFileLocation + ".summary";
        }
        try {
            writeSummary(task, summaryLocation);
            Queues.get(Queues.clientQueueName(task.localAppID))
                    .send(Message.createDoneTask(summaryLocation, task.localAppID));
            servedClients.incrementAndGet();
            logDebug("finished client " + task.localAppID + " (served " + servedClients.get()
                    + ", free slots " + (maxClients - clientTasks.size()) + ")");
        } catch (Exception e) {
            // The summary could not be written or sent: tell the client so it does not hang,
            // and never let this failure poison the manager's state.
            System.err.println("[MANAGER] could not finish client " + task.localAppID
                    + ": " + e.getMessage());
            try {
                String description = e.getMessage() == null
                        ? e.getClass().getSimpleName() : e.getMessage();
                Message failed = Message.createDoneTask("Summary failure: " + description,
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

    /**
     * Publishes this manager's free slots as the EC2 {@code free} tag so clients can pick a
     * manager with room. The tag is only a hint: it is written by this manager alone and may
     * lag behind, so a client can still get a busy reply. A no-op under the in-memory transport.
     */
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
        PrintWriter out = new PrintWriter(new File(summaryLocation));
        try {
            for (Map.Entry<String, String> entry : task.texts.entrySet()) {
                String text = entry.getValue() == null ? "" : entry.getValue();
                text = text.replace('\n', ' ').replace('\r', ' ');
                out.println(entry.getKey() + "\t" + text);
            }
        } finally {
            out.close();
        }
        if (!Queues.isDev()) {
            File file = new File(summaryLocation);
            String s3Key = AWS.S3_PREFIX + "summaries/" + task.localAppID + ".txt";
            AWS.getInstance().getS3().putObject(
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket(AWS.getInstance().bucketName)
                            .key(s3Key)
                            .acl(software.amazon.awssdk.services.s3.model.ObjectCannedACL.PUBLIC_READ)
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

    /**
     * In a real deployment the manager owns {@code done_ocr_<managerID>}; drop it on the way
     * out. In dev the registry is reset by the tests, so there is nothing to delete.
     *
     * Not exercisable offline: this branch only runs with the SQS transport.
     */
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

    private void logDebug(String message) {
        if (client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    /** Per-client work tracked by the manager while its images are being OCRed. */
    static final class ClientTask {
        final String localAppID;
        final String inputFileLocation;
        final LinkedHashMap<String, String> texts = new LinkedHashMap<String, String>();
        int outstanding;
        boolean done;

        ClientTask(String localAppID, String inputFileLocation) {
            this.localAppID = localAppID;
            this.inputFileLocation = inputFileLocation;
        }
    }
}
