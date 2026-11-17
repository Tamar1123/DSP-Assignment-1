package client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.Manager;
import server.Worker;
import utils.Queue;
import utils.Queues;
import utils.Received;

import java.io.File;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One manager and two workers in one JVM, serving two clients' worth of work.
 *
 * Uses the dev (in-memory) transport, so nothing here touches AWS. The clients run one
 * after the other because {@link App} keeps the current input file in a static field; each
 * run still gets its own {@code localAppID} and therefore its own reply queue.
 */
public class EndToEndTest {
    private static final long JOIN_TIMEOUT_MILLIS = 5000;

    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
        App.debug = false;
        App.retryDelayMillis = 0;
        App.terminate = false;
    }

    @Test
    public void twoAppsOneManagerTwoWorkers() throws Exception {
        Manager manager = new Manager();
        Thread managerThread = new Thread(manager, "manager");
        managerThread.start();

        Worker[] workers = new Worker[2];
        Thread[] workerThreads = new Thread[workers.length];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
            workerThreads[i] = new Thread(workers[i], "worker-" + i);
            workerThreads[i].start();
        }

        try {
            File firstInput = inputFile("e2e-a", "http://a1", "http://a2", "http://a3");
            File secondInput = inputFile("e2e-b", "http://b1", "http://b2", "http://b3");

            String firstSummary = runApp(firstInput);
            String secondSummary = runApp(secondInput);

            assertNotNull(firstSummary, "first app received no DONE_TASK");
            assertNotNull(secondSummary, "second app received no DONE_TASK");
            assertTrue(new File(firstSummary).exists(), "missing first summary: " + firstSummary);
            assertTrue(new File(secondSummary).exists(), "missing second summary: " + secondSummary);
            assertNotEquals(firstSummary, secondSummary);

            assertSummaryCovers(firstSummary, "http://a1", "http://a2", "http://a3");
            assertSummaryCovers(secondSummary, "http://b1", "http://b2", "http://b3");
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

    @Test
    public void busyClientBacksOffAndIsAdmittedWhenSlotFrees() throws Exception {
        App.managerClientsN = 1;
        App.workerFilesN = 1;

        final List<Long> directoryLookups = Collections.synchronizedList(new ArrayList<Long>());
        App.managerDirectory = new ManagerDirectory() {
            @Override
            public List<String> freeManagers() {
                directoryLookups.add(Long.valueOf(System.nanoTime()));
                return Arrays.asList("manager-e2e");
            }

            @Override
            public void startManager(int freeSlots) {
                fail("the in-memory test already has a manager");
            }
        };

        Manager manager = new Manager("manager-e2e", 1, 1);
        Worker worker = new Worker();
        File firstInput = inputFile("e2e-first", "http://first");
        File secondInput = inputFile("e2e-second", "http://second");
        CompletableFuture<String> firstSummary = null;
        CompletableFuture<String> secondSummary = null;
        try {
            App.inputFileName = firstInput.getAbsolutePath();
            firstSummary = CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    return App.run();
                }
            });
            Received firstTask = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(firstInput.getAbsolutePath(), firstTask.getMessage().getInputFileLocation());

            App.inputFileName = secondInput.getAbsolutePath();
            secondSummary = CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    return App.run();
                }
            });
            Received secondTask = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(secondInput.getAbsolutePath(), secondTask.getMessage().getInputFileLocation());
            String firstClientID = firstTask.getMessage().getLocalAppID();
            String secondClientID = secondTask.getMessage().getLocalAppID();
            assertNotEquals(firstClientID, secondClientID);

            // Make both requests available in the same receive batch. The first takes the only
            // slot; the second receives the manager's busy reply and must retry as the same client.
            Queue managerQueue = Queues.get(Queues.managerQueueName());
            managerQueue.release(firstTask);
            managerQueue.release(secondTask);
            manager.startOnce();
            assertEquals(1, manager.getActiveClients());
            assertEquals(1, manager.getRejectedClients(), "second client was not told the manager was busy");

            Received retry = managerQueue.receive(1, 5).get(0);
            assertEquals(secondClientID, retry.getMessage().getLocalAppID());
            assertEquals(secondInput.getAbsolutePath(), retry.getMessage().getInputFileLocation());
            assertEquals(3, directoryLookups.size(), "retry must re-check the manager directory");
            managerQueue.release(retry);

            worker.startOnce();
            manager.startOnce(); // done-OCR is processed although the request queue is gated
            assertEquals(0, manager.getActiveClients(), "first completion did not free the slot");
            assertNotNull(firstSummary.get(5, TimeUnit.SECONDS));

            manager.startOnce(); // now the retry is admitted into the freed slot
            assertEquals(1, manager.getActiveClients());
            worker.startOnce();
            manager.startOnce();

            String firstLocation = firstSummary.get(5, TimeUnit.SECONDS);
            String secondLocation = secondSummary.get(5, TimeUnit.SECONDS);
            assertNotNull(firstLocation);
            assertNotNull(secondLocation);
            assertNotEquals(firstLocation, secondLocation);
            assertSummaryCovers(firstLocation, "http://first");
            assertSummaryCovers(secondLocation, "http://second");
            assertEquals(2, manager.getServedClients());
            new File(firstLocation).deleteOnExit();
            new File(secondLocation).deleteOnExit();
        } finally {
            worker.shutdown();
            if (firstSummary != null && !firstSummary.isDone()) {
                firstSummary.cancel(true);
            }
            if (secondSummary != null && !secondSummary.isDone()) {
                secondSummary.cancel(true);
            }
            App.managerDirectory = new ManagerFinder();
        }
    }

    private static String runApp(File inputFile) {
        App.inputFileName = inputFile.getAbsolutePath();
        String summary = App.run();
        if (summary != null) {
            new File(summary).deleteOnExit();
        }
        return summary;
    }

    private static File inputFile(String prefix, String... urls) throws Exception {
        File file = File.createTempFile(prefix, ".txt");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file);
        try {
            for (String url : urls) {
                writer.println(url);
            }
        } finally {
            writer.close();
        }
        return file;
    }

    private static void assertSummaryCovers(String summaryLocation, String... urls) throws Exception {
        String content = read(new File(summaryLocation));
        for (String url : urls) {
            assertTrue(content.contains(url), "summary " + summaryLocation + " is missing " + url);
        }
    }

    private static String read(File file) throws Exception {
        StringBuilder text = new StringBuilder();
        Scanner scanner = new Scanner(file);
        try {
            while (scanner.hasNextLine()) {
                text.append(scanner.nextLine()).append('\n');
            }
        } finally {
            scanner.close();
        }
        return text.toString();
    }
}
