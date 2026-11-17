package client;

import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
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
    static final String MANAGER_JAR_S3_KEY = "Manager.jar";
    static final String WORKER_JAR_S3_KEY = "Worker.jar";

    public static void main(String[] args) {// args = [inputFileName, outputFileName, manager-clients-n, worker-files-n, terminate (optional), -D (Debug, optional)]
        if (args.length < 4) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java -jar <jar> inputFileName outputFileName manager-clients-n worker-files-n [terminate]");
        }
        inputFileName = args[0];
        outputFileName = args[1];
        managerClientsN = Integer.parseInt(args[2]);
        workerFilesN = Integer.parseInt(args[3]);
        for (String arg : args) {
            if (arg.equals("terminate") || arg.equals("terminate")) {
                terminate = true;
            }
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        try {
            if (!Queues.isDev()) {
                setup();
                createEC2();
            }
            String summaryLocation = run();
            if (!Queues.isDev()) {
                S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
                downloadFileFromS3(s3, aws.bucketName, summaryLocation, outputFileName);
                s3.close();
            }
            System.out.println("Finished! Output saved to: " + outputFileName);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Sends the new task to the manager and waits for this client's summary.
     *
     * A per-client reply queue is created before the request is sent, so the manager can
     * never answer before the client is listening. The inbox is deliberately left in place
     * for inspection after the run; it is not reset or deleted here.
     *
     * @return the S3 location of the summary file
     */
    static String run() {
        String localAppID = UUID.randomUUID().toString();
        // Create the inbox before sending the request: in SQS mode a lazily constructed
        // SqsQueue exists only in this JVM, so the manager could otherwise answer into a
        // queue that was never created.
        Queues.create(Queues.clientQueueName(localAppID));
        Queues.create(Queues.managerQueueName());
        Queue inbox = Queues.get(Queues.clientQueueName(localAppID));
        Queue managerQueue = Queues.get(Queues.managerQueueName());

        String fileLocationToSend = inputFileName;
        if (!Queues.isDev()) {
            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            fileLocationToSend = uploadFileToS3(s3, aws.bucketName, inputFileName, localAppID);
            s3.close();
        }
        if (debug)
            System.out.println("[DEBUG] Sending NEW_TASK for " + inputFileName + " as " + localAppID);
        managerQueue.send(Message.createNewTask(inputFileName, localAppID));

        Message done = await(inbox, Message.MessageType.DONE_TASK);

        if (terminate) {
            if (debug)
                System.out.println("[DEBUG] Sending TERMINATE for " + localAppID);
            managerQueue.send(Message.createTerminate(localAppID));
            await(inbox, Message.MessageType.TERMINATE);
        }

        return done.getOutputFileLocation();
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
                // Unexpected traffic on this queue: put it back for the next poll.
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
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        uploadJarToS3(s3, aws.bucketName, MANAGER_JAR_LOCAL_PATH, MANAGER_JAR_S3_KEY);
        uploadJarToS3(s3, aws.bucketName, WORKER_JAR_LOCAL_PATH, WORKER_JAR_S3_KEY);
        s3.close();
    }

    private static void createEC2() {
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        if (isManagerRunning(ec2)) {
            System.out.println("Manager is already running. Skipping EC2 creation.");
            ec2.close();
            return;
        }
        System.out.println("No active Manager found. Launching a new Manager EC2 instance...");
        String ec2Script = "#!/bin/bash\n" +
                "aws s3 cp s3://" + aws.bucketName + "/" + MANAGER_JAR_S3_KEY + " /home/ec2-user/" + MANAGER_JAR_S3_KEY + "\n" +
                "java -jar /home/ec2-user/" + MANAGER_JAR_S3_KEY + " > /home/ec2-user/manager.log 2>&1\n";
        aws.createEC2(ec2Script, "Manager", 1);
        ec2.close();

    private static void uploadJarToS3(S3Client s3, String bucketName, String localPath, String s3Key) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(localPath)));
        System.out.println("[INFO] Uploaded " + s3Key + " to S3.");
    }

    public static boolean isManagerRunning(Ec2Client ec2) {
        Filter stateFilter = Filter.builder()
                .name("instance-state-name")
                .values("running", "pending")
                .build();
        Filter tagFilter = Filter.builder()
                .name("tag:Name")
                .values("Manager")
                .build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(stateFilter, tagFilter)
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                if (debug) {
                    System.out.println("[DEBUG] Found active Manager Instance ID: " + instance.instanceId());
                }
                return true;
            }
        }
        return false;
    }

    public static String uploadFileToS3(S3Client s3, String bucketName, String localFilePath, String localAppID) {
        String s3Key = "input-files/" + localAppID + "-" + Paths.get(localFilePath).getFileName().toString();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
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
}

