import java.util.UUID;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

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
        GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(
        GetQueueUrlRequest.builder().queueName("AppToManager").build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        String localAppID = UUID.randomUUID().toString();

        sendNewTask(sqs, outFilePath, inFilePath, inFilePath);

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

    }
}
