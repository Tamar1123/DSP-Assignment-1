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

    /** Clients with work in flight, keyed by localAppID. */
    private final Map<String, ClientTask> clients = new ConcurrentHashMap<String, ClientTask>();

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

    public String getManagerID() {
        return managerID;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Processes one receive batch from the request queue and one from the done-OCR queue.
     * This is the test seam; {@link #run()} calls it in a loop until {@link #shutdown()} or a
     * terminate message stops the manager.
     */
    public void startOnce() {
        for (Received received : managerQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS)) {
            handle(received, managerQueue);
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

    /** Splits one client input file into one image task per non-empty line. */
    private void handleNewTask(Message msg) throws IOException {
        ClientTask task = new ClientTask(msg.getLocalAppID(), msg.getInputFileLocation());
        for (String url : readUrls(task.inputFileLocation)) {
            workerQueue.send(Message.createNewImageTask(url, task.localAppID, managerID));
            task.outstanding++;
        }
        clients.put(task.localAppID, task);
        if (task.outstanding == 0) {
            finish(task);
        }
    }

    private void handleDoneOcr(Message msg) throws IOException {
        ClientTask task = clients.get(msg.getLocalAppID());
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
        for (ClientTask task : clients.values()) {
            Queues.get(Queues.clientQueueName(task.localAppID))
                    .send(Message.createTerminate(task.localAppID));
        }
        deleteDoneOcrQueueIfAws();
    }

    /**
     * Answers the client once, with the location of the summary. This is a stub for the S3
     * upload: the summary is written next to the input file and its local path is reported.
     */
    private void finish(ClientTask task) throws IOException {
        if (task.done) {
            return;
        }
        String summaryLocation = task.inputFileLocation + ".summary";
        writeSummary(task, summaryLocation);
        Queues.get(Queues.clientQueueName(task.localAppID))
                .send(Message.createDoneTask(summaryLocation, task.localAppID));
        task.done = true;
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
