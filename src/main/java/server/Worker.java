package server;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * A worker: takes one image task at a time from the shared worker queue, OCRs the image and
 * reports the text to the manager that issued the task.
 *
 * Workers share one queue, so any worker may take any task. A completed task is deleted after
 * its reply is sent; a task whose reply could not be sent is released so it is delivered again.
 */
public class Worker implements Runnable {
    private static final int RECEIVE_MAX = 1;
    private static final int RECEIVE_WAIT_SECONDS = 1;

    private final Queue workerQueue;

    private volatile boolean running = true;

    public Worker() {
        this.workerQueue = Queues.get(Queues.workerQueueName());
    }

    public static void main(String[] args) {
        Queues.setDev(false);
        System.out.println("[WORKER] Starting Worker node in AWS SQS mode...");
        Worker worker = new Worker();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[WORKER] Shutting down...");
            worker.shutdown();
        }));

        worker.run();
        System.out.println("[WORKER] Worker stopped.");
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
    public String identify(String imageURL) throws Exception {
        if (Queues.isDev() && (imageURL.startsWith("http://img") || imageURL.startsWith("http://a") || imageURL.startsWith("http://b"))) {
            return "[dev] OCR of " + imageURL;
        }

        Path tempFile = null;
        try {
            tempFile = downloadImage(imageURL);
            return processOne(tempFile.toAbsolutePath().toString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private Path downloadImage(String imageURL) throws Exception {
        URL url = new URL(imageURL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0"); 
        connection.setConnectTimeout(10000); 
        connection.setReadTimeout(15000);

        int status = connection.getResponseCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException("HTTP error " + status + " while downloading image");
        }

        String extension = ".png";
        String pathPart = url.getPath();
        int dotIndex = pathPart.lastIndexOf('.');
        if (dotIndex != -1 && dotIndex < pathPart.length() - 1) {
            String ext = pathPart.substring(dotIndex).toLowerCase();
            if (ext.matches("\\.(png|jpg|jpeg|gif|bmp|tif|tiff)")) {
                extension = ext;
            }
        }

        Path tempFile = Files.createTempFile("ocr_" + UUID.randomUUID(), extension);
        try (InputStream in = connection.getInputStream()) {
            Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        return tempFile;
    }

    private void handle(Received received) {
        Message msg = received.getMessage();

        if (msg.getType() != Message.MessageType.NEW_IMAGE_TASK) {
            // Poison for a worker: no one else will take it, so drop it instead of spinning.
            logDebug("worker dropping message of type " + msg.getType());
            deleteQuietly(received);
            return;
        }

        String text;
        boolean error = false;
        try {
            text = identify(msg.getImageURL());
        } catch (Exception e) {
            // Unreadable image is a completed result: report it and keep the task deleted.
            text = "exception: " + e.getMessage();
            error = true;
        }

        Message reply = Message.createDoneOCR(msg.getImageURL(), text, msg.getLocalAppID());
        reply.setError(error);

        if (sendQuietly(msg.getManagerID(), reply)) {
            deleteQuietly(received);
        } else {
            // The manager never got the answer: give the task back for another delivery.
            releaseQuietly(received);
        }
    }

    private boolean sendQuietly(String managerID, Message reply) {
        try {
            Queues.get(Queues.doneOcrQueueName(managerID)).send(reply);
            return true;
        } catch (Exception e) {
            logDebug("could not reply to manager " + managerID + ": " + e.getMessage());
            return false;
        }
    }

    private void deleteQuietly(Received received) {
        try {
            workerQueue.delete(received);
        } catch (Exception e) {
            // At-least-once: the message returns after the visibility timeout.
            logDebug("delete failed: " + e.getMessage());
        }
    }

    private void releaseQuietly(Received received) {
        try {
            workerQueue.release(received);
        } catch (Exception e) {
            logDebug("release failed; the visibility timeout will return the task: " + e.getMessage());
        }
    }

    private void logDebug(String message) {
        if (client.App.debug) {
            System.out.println("[DEBUG] " + message);
        }
    }

    public static String processOne(String inputFileName) {
        Tesseract tesseract = new Tesseract();
        tesseract.setLanguage("eng");
        String tessDataEnv = System.getenv("TESSDATA_PREFIX");
        if (tessDataEnv != null && !tessDataEnv.trim().isEmpty()) {
            tesseract.setDatapath(tessDataEnv.trim());
        } else {
            File defaultTessdata = new File("/usr/share/tesseract-ocr/4.00/tessdata");
            if (defaultTessdata.isDirectory()) {
                tesseract.setDatapath(defaultTessdata.getAbsolutePath());
            } else {
                File altTessdata = new File("/usr/share/tesseract-ocr/5/tessdata");
                if (altTessdata.isDirectory()) {
                    tesseract.setDatapath(altTessdata.getAbsolutePath());
                }
            }
        }
        try {
            String result = tesseract.doOCR(new File(inputFileName));
            return result == null ? "" : result.trim();
        } catch (TesseractException e) {
            throw new RuntimeException("Tess4J OCR failed: " + e.getMessage(), e);
        }
    }

    

    
}
