package client;

import server.Manager;
import server.Worker;
import utils.Queues;

import java.util.UUID;

/**
 * Dev entry point: one manager and two workers in this JVM, then the real client flow.
 *
 * <b>This is not the graded entry point.</b> The graded run uses {@code client.App}
 * (the jar's {@code Main-Class}) with {@link Queues#DEV} set to {@code false}, so the manager
 * and the workers run as separate processes on EC2 and talk over SQS. This class exists only
 * to exercise the same component code against the in-memory transport, with the queue wiring
 * being the only difference.
 *
 * Usage: {@code java -cp <classes+deps> client.DevRunner input.txt output.html 1 1 [terminate]}
 */
public final class DevRunner {
    private static final int WORKERS = 2;
    private static final long JOIN_TIMEOUT_MILLIS = 5000;

    private DevRunner() {
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            throw new IllegalArgumentException("Not enough arguments. Usage: java -cp <classes+deps> client.DevRunner inputFileName outputFileName manager-clients-n worker-files-n [terminate]");
        }

        // Honour the shared switch: DEV (the default) keeps everything in memory. With DEV
        // false this JVM would use real SQS queues and the local manager/workers would be the
        // only consumers, which is a useful smoke test but not the deployment layout.
        Queues.setDev(Queues.DEV);

        // Same arguments as App.main: honour the grader's inputs instead of the defaults.
        int managerClientsN = Integer.parseInt(args[2]);
        int workerFilesN = Integer.parseInt(args[3]);
        Manager manager = new Manager(UUID.randomUUID().toString(), managerClientsN, workerFilesN);
        Thread managerThread = new Thread(manager, "manager");
        managerThread.start();

        Worker[] workers = new Worker[WORKERS];
        Thread[] workerThreads = new Thread[WORKERS];
        for (int i = 0; i < WORKERS; i++) {
            workers[i] = new Worker();
            workerThreads[i] = new Thread(workers[i], "worker-" + i);
            workerThreads[i].start();
        }

        try {
            // App.main exits the JVM on failure, so the shutdown below covers the normal path
            // (and System.exit skips finally blocks, which is fine: the whole JVM goes away).
            App.execute(args);
        } finally {
            manager.shutdown();
            for (Worker worker : workers) {
                worker.shutdown();
            }
            managerThread.join(JOIN_TIMEOUT_MILLIS);
            for (Thread thread : workerThreads) {
                thread.join(JOIN_TIMEOUT_MILLIS);
            }
        }
    }
}
