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

    /** Pause before resending after a busy reply. Tests set it to 0. */
    static long retryDelayMillis = 5000L;

    static ManagerDirectory managerDirectory = new ManagerFinder();

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
     * Sends the new task and waits for this client's summary.
     * The reply queue is created first so the manager can never answer into a missing queue.
     *
     * @return the S3 location of the summary file
     */
    static String run() {
        String localAppID = UUID.randomUUID().toString();
        Queues.create(Queues.clientQueueName(localAppID));
        Queues.create(Queues.managerQueueName());
        Queue inbox = Queues.get(Queues.clientQueueName(localAppID));
        Queue managerQueue = Queues.get(Queues.managerQueueName());

        // TODO: upload inputFileName to S3 and send the S3 location instead.
        Message done = requestTask(managerQueue, inbox, localAppID, managerDirectory);

        if (terminate) {
            if (debug)
                System.out.println("[DEBUG] Sending TERMINATE for " + localAppID);
            managerQueue.send(Message.createTerminate(localAppID));
            await(inbox, Message.MessageType.TERMINATE);
        }

        return done.getOutputFileLocation();
    }

    /**
     * Sends {@code NEW_TASK} and returns the successful {@code DONE_TASK}.
     * Busy reply (error, no location): the tag was stale, so re-find a manager and resend.
     * Error with a location field: terminal failure, thrown.
     */
    static Message requestTask(Queue managerQueue, Queue inbox, String localAppID,
                               ManagerDirectory directory) {
        while (true) {
            sendNewTask(managerQueue, localAppID, directory);
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
                                    ManagerDirectory directory) {
        boolean started = directory.ensureManager(managerClientsN);
        if (debug)
            System.out.println("[DEBUG] " + (started ? "Started a manager" : "Using a free manager"));
        managerQueue.send(Message.createNewTask(inputFileName, localAppID));
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
            for (Received r : inbox.receive(1, 1)) {
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
