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

    private final String managerID;
    private final Queue managerQueue;
    private final Queue workerQueue;
    private final Queue doneOcrQueue;
    private int maxClients = 10; //default value, will be overwritten by the local App in contruction.

    /** One entry per admitted client (one input file each), keyed by localAppID. Its size is the load. */
    private final Map<String, ClientTask> clientTasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger servedClients = new AtomicInteger(); //cumulative, incremented at finish.
    private final AtomicInteger rejectedClients = new AtomicInteger(); //cumulative, busy replies sent.

    private volatile boolean running = true;

    public Manager() {
        this(UUID.randomUUID().toString());
    }

    public Manager(String managerID) {
        this.managerID = managerID;
        this.managerQueue = Queues.get(Queues.managerQueueName());
        this.workerQueue = Queues.get(Queues.workerQueueName());
        this.doneOcrQueue = Queues.get(Queues.doneOcrQueueName(managerID));
    }

    public Manager(int maxClients, int tasksPerWorker) {
        this(UUID.randomUUID().toString(), maxClients, tasksPerWorker);
    }

    public Manager(String managerID, int maxClients, int tasksPerWorker) {
        this(managerID);
        this.maxClients = maxClients;
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


    public static void main(String[] args) {
        Queues.setDev(false);
        int workerFilesN = 1;
        if (args.length >= 1) {
            try {
                workerFilesN = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("[MANAGER] Invalid worker-files-n argument, defaulting to 1");
            }
        }
        System.out.println("[MANAGER] Starting Manager node in AWS SQS mode (workerFilesN=" + workerFilesN + ")...");
        Manager manager = new Manager();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MANAGER] Shutting down...");
            manager.shutdown();
        }));

        manager.run();
        System.out.println("[MANAGER] Manager stopped.");
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
                return;
            }
            switch (msg.getType()) {
                case NEW_TASK:
                    handleNewTask(msg);
                    break;
                case DONE_OCR:
                    handleDoneOcr(msg);
                    break;
                case TERMINATE:
                    handleTerminate();
                    break;
                default:
                    logDebug("unknown message type on manager queue: " + msg.getType());
                    break;
            }
        } catch (Exception e) {
            // Always drop the request so a poison message cannot spin forever.
            System.err.println("[MANAGER] failed to handle " + msg.getType() + ": " + e.getMessage());
        } finally {
            source.delete(received);
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
        logDebug("admitted client " + localAppID + " with " + task.outstanding + " images");
        if (task.outstanding == 0) {
            finish(task);
        }
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
        String summaryLocation = task.inputFileLocation + ".summary";
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

    private void writeSummary(ClientTask task, String summaryLocation) throws IOException {
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
    }

    private List<String> readUrls(String inputFileLocation) throws IOException {
        List<String> urls = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(new File(inputFileLocation)));
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
