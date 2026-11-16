package client;

import utils.Message;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.util.List;
import java.util.UUID;

public class App {
    final static utils.AWS aws = utils.AWS.getInstance();
    static String inputFileName = "";
    static String outputFileName = "";
    static int managerClientsN  = 0;
    static int workerFilesN = 0;
    public static boolean debug = false;
    static boolean terminate = false;

    public static void main(String[] args) {// args = [inputFileName, outputFileName, manager-clients-n, worker-files-n, terminate (optional), -D (Debug, optional)]
        if (args.length < 4) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java -jar <jar> inputFileName outputFileName manager-clients-n worker-files-n [terminate]");
        }
        inputFileName = args[0];
        outputFileName = args[1];
        managerClientsN = Integer.parseInt(args[2]);
        workerFilesN = Integer.parseInt(args[3]);
        for (String arg : args) {
            if (arg.equals("terminate")) {
                terminate = true;
            }
            if (arg.equals("-D")) {
                debug = true;
            }
        }
        try {
            String summaryLocation = run();
            System.out.println(summaryLocation);
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

        // TODO: upload inputFileName to S3 and send the S3 location instead.
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
            List<Received> received = inbox.receive(1, 1);
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
        if (debug)
            System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
    }

    private static void createEC2() {
        String ec2Script = "#!/bin/bash\n" +
                "echo Hello World\n";
        String managerInstanceID = aws.createEC2(ec2Script, "thisIsJustAString", 1);
    }
}
