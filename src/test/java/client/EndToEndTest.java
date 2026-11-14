package client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.Manager;
import server.Worker;
import utils.Queues;

import java.io.File;
import java.io.PrintWriter;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One manager and two workers in one JVM, serving two clients' worth of work.
 *
 * <p>Uses the dev (in-memory) transport, so nothing here touches AWS. The clients run one
 * after the other because {@link App} keeps the current input file in a static field; each
 * run still gets its own {@code localAppID} and therefore its own reply queue.</p>
 */
public class EndToEndTest {
    private static final long JOIN_TIMEOUT_MILLIS = 5000;

    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
        App.debug = false;
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
