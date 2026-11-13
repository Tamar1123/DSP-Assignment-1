import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import java.nio.file.Paths;

public class App {
    final static AWS aws = AWS.getInstance();
    static String inFilePath = "";
    static String outFilePath = "";
    static int managerClientsN = 0;
    static int workerFilesN = 0;
    static boolean debug = false;
    static boolean terminate = false;

    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)
                                            // -D (Debug, optional)]
        if (args.length < 4) {
            throw new IllegalArgumentException(
                    "Not enough arguments. Usage: java App <inFilePath> <outFilePath> <tasksPerWorker> [-t] [-D]");
        }
        inFilePath = args[0];
        outFilePath = args[1];
        managerClientsN = Integer.parseInt(args[2]);
        workerFilesN = Integer.parseInt(args[3]);
        for (String arg : args) {
            if (arg.equals("-t")) {
                terminate = true;
            }
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        try {
            setup();
            createEC2();
        } catch (Exception e) {
            e.printStackTrace();
        }
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        String localAppID = UUID.randomUUID().toString();
        CreateQueueResponse createQueueResponse = sqs
                .createQueue(CreateQueueRequest.builder().queueName("AppToManager").build());
        String appToManagerQueueUrl = createQueueResponse.queueUrl();
        String responseQueueName = "ManagerToApp-" + localAppID;
        CreateQueueResponse managerToApp = sqs.createQueue(
                CreateQueueRequest.builder().queueName(responseQueueName).build());
        String managerToAppQueueUrl = managerToApp.queueUrl();
        String s3InputKey = uploadFileToS3(s3, aws.bucketName, inFilePath, localAppID);
        sendNewTask(sqs, appToManagerQueueUrl, s3InputKey, localAppID);
        // done_task logic
        String resultS3Key = waitForResult(sqs, managerToAppQueueUrl);
        downloadFileFromS3(s3, aws.bucketName, resultS3Key, outFilePath);
        if (terminate) {
            if (debug) {
                System.out.println("[DEBUG] Terminate flag is set. Sending TERMINATE message to Manager...");
            }
            sendTerminate(sqs, appToManagerQueueUrl, localAppID);
        }
        sqs.close();
        s3.close();
        System.out.println("[INFO] LocalApp finished successfully.");
    }

    public static void sendTerminate(SqsClient sqs, String queueURL, String localAppID) {
        Message msg = Message.createTerminate(localAppID);
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
            .queueUrl(queueURL)
            .messageBody(msg.toJson())
            .build();
        sqs.sendMessage(sendMessageRequest);
    }

    public static void downloadFileFromS3(S3Client s3, String bucketName, String s3Key, String localOutputPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key) 
                .build();
        s3.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get(localOutputPath)));
        System.out.println("[INFO] Summary file downloaded successfully to: " + localOutputPath);
    }

    public static String waitForResult(SqsClient sqs, String managerToAppQueueUrl) {
        while (true) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(managerToAppQueueUrl)
                    .waitTimeSeconds(20)
                    .maxNumberOfMessages(1)
                    .build();
            List<software.amazon.awssdk.services.sqs.model.Message> messages = sqs.receiveMessage(receiveRequest)
                    .messages();
            for (software.amazon.awssdk.services.sqs.model.Message sqsMsg : messages) {
                String body = sqsMsg.body();
                Message message = Message.fromJson(body);
                if (message.getType() == Message.MessageType.DONE_TASK) {
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .queueUrl(managerToAppQueueUrl)
                            .receiptHandle(sqsMsg.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);
                    sqs.deleteQueue(DeleteQueueRequest.builder()
                            .queueUrl(managerToAppQueueUrl)
                            .build());
                    return message.getOutputFileLocation();
                }
            }
        }
    }

    // Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        if (debug)
            System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
    }

    private static void createEC2() {
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        if (isManagerRunning(ec2)) {
            System.out.println("Manager is already running. Skipping EC2 creation.");
            return;
        }
        System.out.println("No active Manager found. Launching a new Manager EC2 instance...");
        String ec2Script = "#!/bin/bash\n" +
                "echo Hello World\n";
        String managerInstanceID = aws.createEC2(ec2Script, "Manager", 1);
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

    public static void sendNewTask(SqsClient sqs, String queueURL, String inFilePath, String localAppID) {
        Message msg = Message.createNewTask(inFilePath, localAppID);
        String jsonMsg = msg.toJson();
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueURL)
                .messageBody(jsonMsg)
                .build();
        SendMessageResponse response = sqs.sendMessage(sendMessageRequest);
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
}
