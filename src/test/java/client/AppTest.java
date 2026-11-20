package client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

public class AppTest {
    private static final long FUTURE_TIMEOUT_SECONDS = 10;

    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
        App.debug = false;
        App.retryDelayMillis = 0;
        App.terminate = false;
        App.inputFileName = "/tmp/in.txt";
        App.outputFileName = "/tmp/out.html";
        App.managerClientsN = 1;
        App.workerFilesN = 1;
        App.managerDirectory = new ManagerFinder();
    }

    /** Records what the client asked for; no EC2, no tags, no AWS. */
    private static final class FakeDirectory implements ManagerDirectory {
        final List<String> free = new ArrayList<String>();
        final List<Integer> startedWith = new ArrayList<Integer>();
        int lookups = 0;

        @Override
        public List<String> freeManagers() {
            lookups++;
            return new ArrayList<String>(free);
        }

        @Override
        public void startManager(int freeSlots) {
            startedWith.add(Integer.valueOf(freeSlots));
            free.add("manager-" + startedWith.size());
        }
    }

    private static Supplier<String> startRun() {
        return new Supplier<String>() {
            @Override
            public String get() {
                return App.run();
            }
        };
    }

    @Test
    void generateHtmlFromSummary_formatsImagesAndErrors() throws Exception {
        Path summaryFile = Files.createTempFile("summary-test-", ".txt");
        Path htmlFile = Files.createTempFile("output-test-", ".html");

        String content = String.join("\n",
                "https://example.com/img1.png\tHello <world> & \"friends\"",
                "https://example.com/bad.png\tERROR: Failed to download image"
        );
        Files.write(summaryFile, content.getBytes(StandardCharsets.UTF_8));

        App.generateHtmlFromSummary(summaryFile, htmlFile);

        String html = new String(Files.readAllBytes(htmlFile), StandardCharsets.UTF_8);
        assertTrue(html.contains("<a href=\"https://example.com/img1.png\"><img src=\"https://example.com/img1.png\""));
        assertTrue(html.contains("Hello &lt;world&gt; &amp; &quot;friends&quot;"));
        assertTrue(html.contains("<a href=\"https://example.com/bad.png\">https://example.com/bad.png</a> Failed to download image"));

        Files.deleteIfExists(summaryFile);
        Files.deleteIfExists(htmlFile);
    }

    @Test
    public void runSendsNewTaskAndReturnsTheSummaryLocation() throws Exception {
        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, task.getMessage().getType());
            assertEquals("/tmp/in.txt", task.getMessage().getInputFileLocation());
            String localAppID = task.getMessage().getLocalAppID();
            assertNotNull(localAppID);
            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
            assertEquals("/tmp/summary.txt", summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            cancelIfRunning(summary);
        }
    }

    @Test
    public void terminateIsSentAfterDoneTask() throws Exception {
        App.terminate = true;
        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, task.getMessage().getType());
            String localAppID = task.getMessage().getLocalAppID();

            // No answer has been sent yet, so nothing else may be on the manager queue: an
            // eagerly sent TERMINATE would be sitting right here.
            assertTrue(Queues.get(Queues.managerQueueName()).receive(1, 0).isEmpty(),
                    "TERMINATE was sent before DONE_TASK");

            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));

            Received term = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.TERMINATE, term.getMessage().getType());

            // The run must not finish before the ack arrives.
            assertFalse(summary.isDone(), "run returned before the TERMINATE ack");

            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createTerminate(localAppID));
            assertEquals("/tmp/summary.txt", summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            cancelIfRunning(summary);
        }
    }

    @Test
    public void freeManagerGetsExactlyOneNewTask() throws Exception {
        FakeDirectory directory = new FakeDirectory();
        directory.free.add("i-free");
        App.managerDirectory = directory;

        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, task.getMessage().getType());
            String localAppID = task.getMessage().getLocalAppID();

            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
            assertEquals("/tmp/summary.txt", summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // A free manager exists, so no instance may be started and no second task sent.
            assertEquals(Collections.<Integer>emptyList(), directory.startedWith);
            assertTrue(Queues.get(Queues.managerQueueName()).receive(1, 0).isEmpty(),
                    "more than one NEW_TASK was sent");
        } finally {
            cancelIfRunning(summary);
        }
    }

    @Test
    public void busyReplyTriggersReselectionAndRetry() throws Exception {
        FakeDirectory directory = new FakeDirectory();
        // A stale tag: the first manager looks free but answers busy, and still looks free on
        // the second lookup. The client must re-read the directory before re-sending.
        directory.free.add("i-stale");
        App.managerDirectory = directory;

        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received first = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, first.getMessage().getType());
            String localAppID = first.getMessage().getLocalAppID();

            Message busy = Message.createDoneTask(null, localAppID);
            busy.setError(true);
            Queues.get(Queues.clientQueueName(localAppID)).send(busy);

            Received second = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, second.getMessage().getType());
            assertEquals(localAppID, second.getMessage().getLocalAppID(),
                    "the retry must be for the same client, so a busy manager left no state behind");
            assertTrue(directory.lookups >= 2, "the directory was not re-read after the busy reply");
            assertEquals(Collections.<Integer>emptyList(), directory.startedWith);

            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
            assertEquals("/tmp/summary.txt", summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue(Queues.get(Queues.managerQueueName()).receive(1, 0).isEmpty(),
                    "a third NEW_TASK was sent");
        } finally {
            cancelIfRunning(summary);
        }
    }

    @Test
    public void terminalErrorReplyFailsWithoutRetrying() throws Exception {
        FakeDirectory directory = new FakeDirectory();
        directory.free.add("i-free");
        App.managerDirectory = directory;

        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            String localAppID = task.getMessage().getLocalAppID();
            Message failed = Message.createDoneTask("Summary write failed: disk full", localAppID);
            failed.setError(true);
            Queues.get(Queues.clientQueueName(localAppID)).send(failed);

            java.util.concurrent.ExecutionException thrown = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(thrown.getCause() instanceof IllegalStateException);
            assertTrue(thrown.getCause().getMessage().contains("disk full"));
            assertTrue(Queues.get(Queues.managerQueueName()).receive(1, 0).isEmpty(),
                    "terminal manager failure must not trigger another NEW_TASK");
            assertEquals(Collections.<Integer>emptyList(), directory.startedWith);
        } finally {
            cancelIfRunning(summary);
        }
    }

    @Test
    public void noFreeManagerStartsOneAndSendsTheTask() throws Exception {
        final FakeDirectory directory = new FakeDirectory();
        App.managerDirectory = directory;

        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.NEW_TASK, task.getMessage().getType());
            assertEquals(Arrays.asList(Integer.valueOf(App.managerClientsN)), directory.startedWith,
                    "exactly one manager must be started, with manager-clients-n free slots");
            String localAppID = task.getMessage().getLocalAppID();

            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
            assertEquals("/tmp/summary.txt", summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            cancelIfRunning(summary);
        }
    }

    private static void cancelIfRunning(CompletableFuture<String> future) {
        if (!future.isDone()) {
            future.cancel(true);
        }
    }
}
