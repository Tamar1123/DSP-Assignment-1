package server;

import com.asprise.ocr.Ocr;
import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.io.File;

/**
 * A worker: takes one image task at a time from the shared worker queue, OCRs the image and
 * reports the text to the manager that issued the task.
 *
 * Workers share one queue, so any worker may take any task. A message is deleted only
 * after a reply has been attempted; a failed OCR still produces an error reply, and a failed
 * reply leaves the worker alive for the next task.
 */
public class Worker implements Runnable {
    private static final int RECEIVE_MAX = 1;
    private static final int RECEIVE_WAIT_SECONDS = 1;

    private final Queue workerQueue;

    private volatile boolean running = true;

    public Worker() {
        this.workerQueue = Queues.get(Queues.workerQueueName());
    }

    @Override
    public void run() {
        while (running) {
            try {
                startOnce();
            } catch (Exception e) {
                // Keep the worker alive: one bad batch must not take the whole node down.
                System.err.println("[WORKER] receive loop error: " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Processes one receive batch from the worker queue. This is the test seam;
     * {@link #run()} calls it in a loop until {@link #shutdown()}.
     */
    public void startOnce() {
        for (Received received : workerQueue.receive(RECEIVE_MAX, RECEIVE_WAIT_SECONDS)) {
            handle(received);
        }
    }

    /**
     * Produces the text for one image. The real implementation has two steps: download the
     * image at {@code imageURL} to a local file, then return
     * {@link #processOne(String) processOne(localFile.getAbsolutePath())}.
     */
    public String identify(String imageURL) {
        // TODO: download imageURL to a local file, then call processOne(localFile path).
        // Those are the two real steps; this stub keeps the pipeline offline-testable.
        return "[dev] OCR of " + imageURL;
    }

    private void handle(Received received) {
        Message msg = received.getMessage();
        try {
            if (msg.getType() != Message.MessageType.NEW_IMAGE_TASK) {
                logDebug("worker dropping message of type " + msg.getType());
                return;
            }

            String text;
            boolean error = false;
            try {
                text = identify(msg.getImageURL());
            } catch (Exception e) {
                text = "exception: " + e.getMessage();
                error = true;
            }

            Message reply = Message.createDoneOCR(msg.getImageURL(), text, msg.getLocalAppID());
            reply.setError(error);
            try {
                Queues.get(Queues.doneOcrQueueName(msg.getManagerID())).send(reply);
            } catch (Exception e) {
                // The manager may be gone; that must not stop this worker.
                logDebug("could not reply to manager " + msg.getManagerID() + ": " + e.getMessage());
            }
        } finally {
            // Always drop the task: a reply was attempted on every path, so leaving it in the
            // queue would only cause a duplicate OCR after the visibility timeout.
            workerQueue.delete(received);
        }
    }

    private void logDebug(String message) {
        if (client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    public static String processOne(String inputFileName) {
        Ocr.setUp(); // one time setup
        Ocr ocr = new Ocr(); // create a new OCR engine
        ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
        String s = ocr.recognize(new File[] {new File(inputFileName)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
        ocr.stopEngine();
        return s;
    }
}
