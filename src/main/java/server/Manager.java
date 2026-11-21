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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The manager: routes client work to the shared worker queue and answers each client once
 * all of its images have been OCRed.
 *
 * The manager runs exactly two threads. The main thread is the {@link #routerTick() Router}:
 * it only receives messages, updates shared state and enqueues work — it never touches S3 or
 * EC2. The single {@link #ioWorkerLoop() IoWorker} thread does every blocking call: reading
 * input files, sending image tasks, bootstrapping workers, writing summaries and answering
 * clients. Jobs sit in one priority queue in which finishes outrank fan-outs, so a completed
 * client is never held up by another client's fan-out.
 *
 * The manager never OCRs and never downloads images. It owns one request queue (shared by
 * all clients), one shared worker queue, and one {@code done_ocr_<managerID>} queue that its
 * workers report to.
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

    /** One entry per admitted client (one input file each), keyed by localAppID. Its size is the load. */
    private final Map<String, ClientTask> clientTasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger servedClients = new AtomicInteger(); //cumulative, incremented at finish.
    private final AtomicInteger rejectedClients = new AtomicInteger(); //cumulative, busy replies sent.

    /**
     * IoWorker jobs in priority order: finishes (0) before fan-outs (1), FIFO within a
     * priority. The IoWorker thread takes from it; tests may drain it synchronously instead.
     */
    private final PriorityBlockingQueue<IoJob> ioJobs = new PriorityBlockingQueue<IoJob>();

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
     * One router tick. This is the test seam; {@link #run()} calls it in a loop until
     * {@link #shutdown()} or a terminate message stops the manager.
     *
     * First the done-OCR queue is drained without waiting, so results are recorded and finish
     * jobs are queued even while the manager is full. Then the request queue is polled —
     * always, full or not: a duplicate is ignored, a request without a free slot is answered
     * with a busy error DONE_TASK and deleted, and only a real free slot admits a client.
     * Admission never performs S3 or EC2 I/O; the blocking part is submitted to the IoWorker.
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

    /**
     * The synchronous test seam: one router tick followed by running every pending IoWorker
     * job inline (finishes before fan-outs). Existing tests drive the manager with this and
     * stay deterministic; the concurrency tests use {@link #routerTick()} and
     * {@link #runOnePendingJob()} to interleave the two roles themselves.
     */
    public void startOnce() {
        routerTick();
        runPendingIo();
    }

    /** Runs every pending IoWorker job inline, finishes before fan-outs. Test seam. */
    public void runPendingIo() {
        IoJob job;
        while ((job = ioJobs.poll()) != null) {
            runJob(job);
        }
    }

    /**
     * Runs one pending IoWorker job inline (a finish if one is queued, else a fan-out).
     * Test seam: a test thread calling this acts as the IoWorker while the test thread
     * drives the router. Returns true when a job ran.
     */
    public boolean runOnePendingJob() {
        IoJob job = ioJobs.poll();
        if (job == null) {
            return false;
        }
        runJob(job);
        return true;
    }

    /**
     * Runs every queued finish now. The fan-out calls this between batches so a client whose
     * last image has been OCRed is never kept waiting behind the rest of another client's
     * fan-out; tests call it directly to prove that ordering.
     */
    public void runPendingFinishes() {
        IoJob job;
        while ((job = ioJobs.peek()) != null && job.priority() == IoJob.FINISH) {
            if (ioJobs.poll() != job) {
                break; // another consumer took the head job
            }
            runJob(job);
        }
    }

    /**
     * Starts the manager: the single IoWorker thread plus the Router loop on this thread.
     * The launch arguments are the two values the client knows:
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
        Thread ioWorker = new Thread(new Runnable() {
            public void run() {
                ioWorkerLoop();
            }
        }, "manager-io-worker-" + managerID);
        ioWorker.setDaemon(true);
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
            ioJobs.add(STOP);
        }
    }

    /** The single IoWorker thread: take the highest-priority job and run it, until stopped. */
    private void ioWorkerLoop() {
        while (true) {
            IoJob job;
            try {
                job = ioJobs.take();
            } catch (InterruptedException e) {
                return;
            }
            if (job == STOP) {
                return;
            }
            runJob(job);
        }
    }

    private void runJob(IoJob job) {
        try {
            job.run();
        } catch (Exception e) {
            // One bad job must not kill the IoWorker; the job itself reports failures to the client.
            System.err.println("[MANAGER] io job failed: " + e.getMessage());
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
     * Admission (Router only). A duplicate for an admitted client is ignored; a request that
     * arrives when every slot is taken is answered busy and deleted; otherwise the task is
     * stored and its input read, image-task sends and worker bootstrap are submitted to the
     * IoWorker as one fan-out job. No S3 or EC2 call happens on this path.
     */
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
        ioJobs.add(new FanOutJob(task));
        logDebug("admitted client " + localAppID);
    }

    /** Records one OCR result (first text wins) and queues the finish at zero outstanding. */
    private void handleDoneOcr(Message msg) {
        ClientTask task = clientTasks.get(msg.getLocalAppID());
        if (task == null) {
            logDebug("ignoring done OCR for unknown client " + msg.getLocalAppID());
            return;
        }
        // Keep the first text for a URL, but count every result. Duplicate input URLs are
        // legitimate separate tasks; without a per-task id in the wire format, an SQS
        // redelivery cannot be told apart from a second identical-URL task.
        task.texts.putIfAbsent(msg.getImageURL(), msg.getIdentifiedText());
        if (task.outstanding.decrementAndGet() <= 0) {
            ioJobs.add(new FinishJob(task));
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

    /**
     * Removes a failed task, frees its slot and answers the client with an error — exactly
     * once, whatever failed. Runs on the IoWorker only: removals and tag writes happen there.
     */
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

    private static String describe(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    private void logDebug(String message) {
        if (client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    /**
     * A unit of blocking work for the IoWorker. Finishes have the higher priority, so an
     * answerable client is never queued behind another client's fan-out; jobs of one priority
     * keep their submission order.
     */
    abstract static class IoJob implements Runnable, Comparable<IoJob> {
        static final int FINISH = 0;
        static final int FAN_OUT = 1;
        static final int STOP = Integer.MAX_VALUE;

        private static final AtomicLong SEQUENCE = new AtomicLong();

        private final int priority;
        private final long sequence = SEQUENCE.getAndIncrement();

        IoJob(int priority) {
            this.priority = priority;
        }

        int priority() {
            return priority;
        }

        @Override
        public int compareTo(IoJob other) {
            int byPriority = Integer.compare(priority, other.priority);
            return byPriority != 0 ? byPriority : Long.compare(sequence, other.sequence);
        }
    }

    /** Poisons the IoWorker loop so {@link #run()} can stop it after a graceful drain. */
    private static final IoJob STOP = new IoJob(IoJob.STOP) {
        @Override
        public void run() {
        }
    };

    /**
     * Reads the input file once, records the immutable URL list, sends one NEW_IMAGE_TASK per
     * URL in batches of ten — running pending finishes between batches — and then boots the
     * workers. An empty input finishes the client at once. A failure here removes the task,
     * frees the slot and answers the client with an error.
     */
    private final class FanOutJob extends IoJob {
        private final ClientTask task;

        FanOutJob(ClientTask task) {
            super(IoJob.FAN_OUT);
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
                    ioJobs.add(new FinishJob(task)); // empty input finishes at once
                }
                ensureWorkers(totalOutstanding());
            } catch (Exception e) {
                failTask(task, "Input failure: " + describe(e));
            }
        }
    }

    /**
     * Answers the client once: writes the summary in input URL order, uploads it, sends
     * DONE_TASK, removes the task and updates the free-slot tag. A failure still answers the
     * client (with an error), still frees the slot and still runs only once — the CAS on
     * {@code done} makes finish idempotent.
     */
    private final class FinishJob extends IoJob {
        private final ClientTask task;

        FinishJob(ClientTask task) {
            super(IoJob.FINISH);
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

    /** Per-client work tracked by the manager while its images are being OCRed. */
    static final class ClientTask {
        final String localAppID;
        final String inputFileLocation;
        /** First text received per URL wins; a duplicate DONE_OCR must not overwrite it. */
        final ConcurrentHashMap<String, String> texts = new ConcurrentHashMap<String, String>();
        /** Image tasks not yet answered; set by the fan-out, decremented by the Router. */
        final AtomicInteger outstanding = new AtomicInteger();
        /** CAS guard so the client is answered exactly once, even across failure paths. */
        final AtomicBoolean done = new AtomicBoolean(false);
        /** Immutable URL list in input order; set before the first image task is sent. */
        volatile List<String> urls = Collections.emptyList();

        ClientTask(String localAppID, String inputFileLocation) {
            this.localAppID = localAppID;
            this.inputFileLocation = inputFileLocation;
        }
    }
}
