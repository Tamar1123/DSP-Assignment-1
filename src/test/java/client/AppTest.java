package client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        App.terminate = false;
        App.WAIT_TIMEOUT_SECONDS = 60;
        App.inputFileName = "/tmp/in.txt";
        App.outputFileName = "/tmp/out.html";
        App.managerClientsN = 1;
        App.workerFilesN = 1;
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
    public void terminateAckTimeoutIsReportedNotHung() throws Exception {
        App.terminate = true;
        App.WAIT_TIMEOUT_SECONDS = 1;
        CompletableFuture<String> summary = CompletableFuture.supplyAsync(startRun());
        try {
            Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            String localAppID = task.getMessage().getLocalAppID();
            Queues.get(Queues.clientQueueName(localAppID))
                    .send(Message.createDoneTask("/tmp/summary.txt", localAppID));

            Received term = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
            assertEquals(Message.MessageType.TERMINATE, term.getMessage().getType());

            // Deliberately no ack: the client must report the timeout, not hang.
            try {
                summary.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fail("expected the run to time out waiting for the TERMINATE ack");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                while (cause instanceof java.util.concurrent.CompletionException
                        && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertTrue(cause instanceof IllegalStateException,
                        "expected an IllegalStateException cause, got " + cause);
            }
        } finally {
            cancelIfRunning(summary);
            App.WAIT_TIMEOUT_SECONDS = 60;
        }
    }

    @Test
    public void timeoutIsReportedNotHung() {
        App.WAIT_TIMEOUT_SECONDS = 1;
        try {
            assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
                @Override
                public void execute() {
                    App.run();
                }
            });
        } finally {
            App.WAIT_TIMEOUT_SECONDS = 60;
        }
    }

    private static void cancelIfRunning(CompletableFuture<String> future) {
        if (!future.isDone()) {
            future.cancel(true);
        }
    }
}
