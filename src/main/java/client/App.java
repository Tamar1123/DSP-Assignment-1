package client;

import utils.AWS;
import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class App {
    final static utils.AWS aws = utils.AWS.getInstance();
    static String inputFileName = "";
    static String outputFileName = "";
    static int managerClientsN  = 0;
    static int workerFilesN = 0;
    public static boolean debug = false;
    static boolean terminate = false;

    static final String MANAGER_JAR_LOCAL_PATH = "target/Manager.jar";
    static final String WORKER_JAR_LOCAL_PATH = "target/Worker.jar";
    static final String MANAGER_JAR_S3_KEY = AWS.S3_PREFIX + "Manager.jar";
    static final String WORKER_JAR_S3_KEY = AWS.S3_PREFIX + "Worker.jar";

    /** Pause before resending after a busy reply. Tests set it to 0. */
    static long retryDelayMillis = 5000L;

    static ManagerDirectory managerDirectory = new ManagerFinder();

    public static void main(String[] args) {// args = [inputFileName, outputFileName, manager-clients-n, worker-files-n, terminate (optional), -D (Debug, optional)]
        Queues.setDev(false);
        execute(args);
    }

    static void execute(String[] args) {
        if (args.length < 4) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java -jar <jar> inputFileName outputFileName manager-clients-n worker-files-n [terminate]");
        }
        inputFileName = args[0];
        outputFileName = args[1];
        managerClientsN = Integer.parseInt(args[2]);
        workerFilesN = Integer.parseInt(args[3]);
        if (managerClientsN < 1 || workerFilesN < 1) {
            throw new IllegalArgumentException("manager-clients-n and worker-files-n must be >= 1");
        }
        terminate = false;
        debug = false;
        for (String arg : args) {
            if (arg.equals("terminate")) {
                terminate = true;
            }
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        try {
            if (!Queues.isDev()) {
                setup();
            }
            String summaryLocation = run();
            Path localSummaryPath;
            boolean deleteTempSummary = false;
            if (!Queues.isDev()) {
                localSummaryPath = Files.createTempFile("ocr-summary-", ".txt");
                deleteTempSummary = true;
                downloadFileFromS3(aws.getS3(), aws.bucketName, summaryLocation, localSummaryPath.toString());
            }
            else {
                localSummaryPath = Paths.get(summaryLocation);
            }
            generateHtmlFromSummary(localSummaryPath, Paths.get(outputFileName));
            if (deleteTempSummary) {
                Files.deleteIfExists(localSummaryPath);
            }
            System.out.println("Finished! Output saved to: " + outputFileName);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Sends the new task and waits for this client's summary.
     * The reply queue is created first so the manager can never answer into a missing queue.
     * In a real run the input is uploaded to S3 first and that location is what the manager
     * receives; the manager cannot read this machine's filesystem.
     *
     * @return the S3 location of the summary file
     */
    static String run() {
        String localAppID = UUID.randomUUID().toString();
        String clientQueueName = Queues.clientQueueName(localAppID);
        Queues.create(clientQueueName);
        Queues.create(Queues.managerQueueName());
        Queue inbox = Queues.get(clientQueueName);
        Queue managerQueue = Queues.get(Queues.managerQueueName());
        try {
            String fileLocationToSend = inputFileName;
            if (!Queues.isDev()) {
                fileLocationToSend = uploadFileToS3(aws.getS3(), aws.bucketName, inputFileName, localAppID);
            }
            if (debug)
                System.out.println("[DEBUG] Sending NEW_TASK for " + fileLocationToSend + " as " + localAppID);
            Message done = requestTask(managerQueue, inbox, localAppID, managerDirectory, fileLocationToSend);
            if (terminate) {
                if (debug)
                    System.out.println("[DEBUG] Sending TERMINATE for " + localAppID);
                managerQueue.send(Message.createTerminate(localAppID));
                await(inbox, Message.MessageType.TERMINATE);
            }
            return done.getOutputFileLocation();
        } finally {
            deleteClientQueueIfAws(clientQueueName);
        }
    }

    private static void deleteClientQueueIfAws(String clientQueueName) {
        if (Queues.isDev()) {
            return;
        }
        try {
            String url = aws.createQueue(clientQueueName);
            aws.deleteQueue(url);
        } catch (Exception e) {
            System.err.println("[WARN] Could not delete client queue " + clientQueueName + ": " + e.getMessage());
        }
    }

    /**
     * Sends {@code NEW_TASK} and returns the successful {@code DONE_TASK}.
     * Busy reply (error, no location): the tag was stale, so re-find a manager and resend.
     * Error with a location field: terminal failure, thrown.
     */
    static Message requestTask(Queue managerQueue, Queue inbox, String localAppID,
                               ManagerDirectory directory, String fileLocationToSend) {
        while (true) {
            sendNewTask(managerQueue, localAppID, directory, fileLocationToSend);
            Message reply = await(inbox, Message.MessageType.DONE_TASK);
            if (!reply.isError()) {
                return reply;
            }
            if (reply.getOutputFileLocation() != null) {
                throw new IllegalStateException("Manager failed to complete task: "
                        + reply.getOutputFileLocation());
            }
            if (debug)
                System.out.println("[DEBUG] Manager busy, retrying " + localAppID);
            pauseBeforeRetry();
        }
    }

    /** Makes sure a manager is available, then sends the request. */
    private static void sendNewTask(Queue managerQueue, String localAppID,
                                    ManagerDirectory directory, String fileLocationToSend) {
        boolean started = directory.ensureManager(managerClientsN);
        if (debug)
            System.out.println("[DEBUG] " + (started ? "Started a manager" : "Using a free manager"));
        managerQueue.send(Message.createNewTask(fileLocationToSend, localAppID));
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(retryDelayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to retry", e);
        }
    }

    /** Polls {@code inbox} until a message of {@code expected} arrives. */
    private static Message await(Queue inbox, Message.MessageType expected) {
        while (true) {
            List<Received> received = inbox.receive(1, 20);
            for (Received r : received) {
                if (r.getMessage().getType() == expected) {
                    inbox.delete(r);
                    return r.getMessage();
                }
                // Not ours to consume: put it back.
                inbox.release(r);
            }
        }
    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        if (debug) {
            System.out.println("[DEBUG] Create bucket if not exist and upload JARs.");
        }
        aws.createBucketIfNotExists(aws.bucketName);
    }

    /**
     * Starts one manager with {@code freeSlots} free client slots. The instance downloads the
     * jar uploaded by {@link #setup()} and runs {@code server.Manager} in the background.
     */
    static void createEC2(int freeSlots) {
        S3Client s3 = aws.getS3();
        uploadJarToS3(s3, aws.bucketName, MANAGER_JAR_LOCAL_PATH, MANAGER_JAR_S3_KEY);
        uploadJarToS3(s3, aws.bucketName, WORKER_JAR_LOCAL_PATH, WORKER_JAR_S3_KEY);

        String ec2Script = "#!/bin/bash\n" +
                "TOKEN=$(curl -X PUT \"http://169.254.169.254/latest/api/token\" -H \"X-aws-ec2-metadata-token-ttl-seconds: 21600\" -s)\n" +
                "INSTANCE_ID=$(curl -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/instance-id -s)\n" +
                "aws s3 cp s3://" + aws.bucketName + "/" + MANAGER_JAR_S3_KEY + " /home/ec2-user/Manager.jar\n" +
                "java -jar /home/ec2-user/Manager.jar " + managerClientsN + " " + workerFilesN + " \"$INSTANCE_ID\" > /home/ec2-user/manager.log 2>&1\n";
        aws.createEC2(ec2Script, "manager", 1, freeSlots);
    }

    private static void uploadJarToS3(S3Client s3, String bucketName, String localPath, String s3Key) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .acl(ObjectCannedACL.PUBLIC_READ)
                .build();
        s3.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(localPath)));
        System.out.println("[INFO] Uploaded " + s3Key + " to S3.");
    }

    public static String uploadFileToS3(S3Client s3, String bucketName, String localFilePath, String localAppID) {
        String s3Key = AWS.S3_PREFIX + "input-files/" + localAppID + "-" + Paths.get(localFilePath).getFileName().toString();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .acl(ObjectCannedACL.PUBLIC_READ)
                .build();
        s3.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(localFilePath)));
        System.out.println("File uploaded successfully to S3: " + s3Key);
        return s3Key;
    }

    public static void downloadFileFromS3(S3Client s3, String bucketName, String s3Key, String localOutputPath) throws Exception {
        Files.deleteIfExists(Paths.get(localOutputPath));
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get(localOutputPath)));
        System.out.println("Summary file downloaded successfully to: " + localOutputPath);
    }

    public static void generateHtmlFromSummary(Path summaryPath, Path htmlOutputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(summaryPath, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(htmlOutputPath, StandardCharsets.UTF_8)) {
            writer.write("<!DOCTYPE html>\n<html>\n<head><meta charset=\"UTF-8\"><title>OCR Results</title></head>\n<body>\n");
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                int tabIndex = line.indexOf('\t');
                String url = tabIndex >= 0 ? line.substring(0, tabIndex) : line;
                String text = tabIndex >= 0 ? line.substring(tabIndex + 1) : "";
                String escapedUrl = escapeHtml(url);

                if (text.startsWith("ERROR:")) {
                    String description = escapeHtml(text.substring("ERROR:".length()).trim());
                    writer.write("<p><a href=\"" + escapedUrl + "\">" + escapedUrl + "</a> " + description + "</p>\n");
                } else {
                    String escapedText = escapeHtml(text);
                    writer.write("<p><a href=\"" + escapedUrl + "\"><img src=\"" + escapedUrl + "\" alt=\"image\"></a><br>" + escapedText + "</p>\n");
                }
            }
            writer.write("</body>\n</html>\n");
        }
    }

    private static String escapeHtml(String s) {
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
