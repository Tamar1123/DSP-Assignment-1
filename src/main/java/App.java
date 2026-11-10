import java.util.UUID;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import java.nio.file.Paths;

public class App {
    final static AWS aws = AWS.getInstance();
    static String inFilePath = "";
    static String outFilePath = "";
    static int managerClientsN  = 0;
    static int workerFilesN = 0;
    static boolean debug = false;
    static boolean terminate = false;
    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional) -D (Debug, optional)]
        if (args.length < 4) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java App <inFilePath> <outFilePath> <tasksPerWorker> [-t] [-D]");
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
        CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder().queueName("AppToManager").build());
        String queueUrl = createQueueResponse.queueUrl();
        String localAppID = UUID.randomUUID().toString();

        sendNewTask(sqs, queueUrl, inFilePath, localAppID);

    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        if (debug)
            System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
    }

    private static void createEC2() {
        String ec2Script = "#!/bin/bash\n" +
                "echo Hello World\n";
        String managerInstanceID = aws.createEC2(ec2Script, "thisIsJustAString", 1);
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

        System.out.println("[INFO] File uploaded successfully to S3: " + s3Key);
        return s3Key;
    }
}
