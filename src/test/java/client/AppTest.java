package client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class AppTest {
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

    @Test
    public void runSendsNewTaskAndReturnsTheSummaryLocation() throws Exception {
        CompletableFuture<String> summary = CompletableFuture.supplyAsync(new java.util.function.Supplier<String>() {
            @Override
            public String get() {
                return App.run();
            }
        });
        Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
        assertEquals(Message.MessageType.NEW_TASK, task.getMessage().getType());
        assertEquals("/tmp/in.txt", task.getMessage().getInputFileLocation());
        String localAppID = task.getMessage().getLocalAppID();
        assertNotNull(localAppID);
        Queues.get(Queues.clientQueueName(localAppID))
                .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
        assertEquals("/tmp/summary.txt", summary.get());
    }

    @Test
    public void terminateIsSentAfterDoneTask() throws Exception {
        App.terminate = true;
        CompletableFuture<String> summary = CompletableFuture.supplyAsync(new java.util.function.Supplier<String>() {
            @Override
            public String get() {
                return App.run();
            }
        });
        Received task = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
        String localAppID = task.getMessage().getLocalAppID();
        Queues.get(Queues.clientQueueName(localAppID))
                .send(Message.createDoneTask("/tmp/summary.txt", localAppID));
        Received term = Queues.get(Queues.managerQueueName()).receive(1, 5).get(0);
        assertEquals(Message.MessageType.TERMINATE, term.getMessage().getType());
        Queues.get(Queues.clientQueueName(localAppID)).send(Message.createTerminate(localAppID));
        assertEquals("/tmp/summary.txt", summary.get());
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
}
