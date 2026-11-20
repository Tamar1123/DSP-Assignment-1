package server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ManagerTest {
    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
    }

    private String inputFile(String... urls) throws Exception {
        File f = File.createTempFile("input", ".txt");
        f.deleteOnExit();
        PrintWriter w = new PrintWriter(f);
        for (String u : urls) {
            w.println(u);
        }
        w.close();
        return f.getAbsolutePath();
    }

    @Test
    public void newTaskProducesOneImageTaskPerLine() throws Exception {
        String in = inputFile("http://a", "http://b");
        Manager m = new Manager("mgr-1");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-1"));
        m.startOnce();
        List<Received> tasks = Queues.get(Queues.workerQueueName()).receive(10, 1);
        assertEquals(2, tasks.size());
        assertEquals("mgr-1", tasks.get(0).getMessage().getManagerID());
        assertEquals("app-1", tasks.get(0).getMessage().getLocalAppID());
    }

    @Test
    public void emptyInputAnswersImmediately() throws Exception {
        String in = inputFile();
        Manager m = new Manager("mgr-2");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-2"));
        m.startOnce();
        List<Received> done = Queues.get(Queues.clientQueueName("app-2")).receive(1, 1);
        assertEquals(1, done.size());
        assertEquals(Message.MessageType.DONE_TASK, done.get(0).getMessage().getType());
    }

    @Test
    public void duplicateUrlsAreTwoTasks() throws Exception {
        String in = inputFile("http://a", "http://a");
        Manager m = new Manager("mgr-3");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-3"));
        m.startOnce();
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        for (int i = 0; i < 2; i++) {
            Queues.get(Queues.doneOcrQueueName("mgr-3"))
                    .send(Message.createDoneOCR("http://a", "text" + i, "app-3"));
        }
        m.startOnce();
        assertEquals(1, Queues.get(Queues.clientQueueName("app-3")).receive(1, 1).size());
    }

    @Test
    public void doneOcrFromUnknownClientIsIgnored() {
        Manager m = new Manager("mgr-4");
        Queues.get(Queues.doneOcrQueueName("mgr-4"))
                .send(Message.createDoneOCR("http://a", "t", "nobody"));
        m.startOnce();
        assertTrue(Queues.get(Queues.clientQueueName("nobody")).receive(1, 1).isEmpty());
        assertEquals(0, m.getServedClients());
        assertEquals(0, m.getActiveClients());
    }

    @Test
    public void terminateAcksServedClientsAndStops() throws Exception {
        // app-5 is still in flight (one image outstanding), so it is in the map and gets the ack.
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-5");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-5"));
        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-5"));
        m.startOnce();
        List<Received> acks = Queues.get(Queues.clientQueueName("app-5")).receive(10, 2);
        assertTrue(acks.stream().anyMatch(r -> r.getMessage().getType() == Message.MessageType.TERMINATE));
        assertFalse(m.isRunning());
    }

    @Test
    public void malformedMessageIsDroppedAndLoopContinues() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-6");
        // a poison message: valid JSON, unknown type
        Message poison = Message.createDoneTask("nowhere", "app-6");
        Queues.get(Queues.managerQueueName()).send(poison);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-6"));
        m.startOnce();
        assertEquals(1, Queues.get(Queues.workerQueueName()).receive(10, 2).size());
    }

    @Test
    public void admitsUpToMaxClientsAndAnswersTheNextBusy() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-7", 2, 5);
        for (String app : new String[] {"app-7a", "app-7b", "app-7c"}) {
            Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, app));
        }
        // All three arrive in one batch: the gate was open when the batch was received.
        m.startOnce();
        assertEquals(2, m.getActiveClients());
        assertFalse(m.hasCapacity());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());

        List<Received> busy = Queues.get(Queues.clientQueueName("app-7c")).receive(1, 1);
        assertEquals(1, busy.size());
        assertEquals(Message.MessageType.DONE_TASK, busy.get(0).getMessage().getType());
        assertTrue(busy.get(0).getMessage().isError());
        assertNull(busy.get(0).getMessage().getOutputFileLocation());
        assertEquals(1, m.getRejectedClients());
        assertEquals(0, m.getServedClients());
    }

    @Test
    public void fullManagerDoesNotPollTheRequestQueue() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-8", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-8a"));
        m.startOnce();
        assertFalse(m.hasCapacity());

        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-8b"));
        m.startOnce();
        // Left on the request queue for a manager with room, not answered busy here.
        assertTrue(Queues.get(Queues.clientQueueName("app-8b")).receive(1, 1).isEmpty());
        assertEquals(0, m.getRejectedClients());
        assertEquals(1, Queues.get(Queues.managerQueueName()).receive(10, 1).size());
    }

    @Test
    public void finishFreesTheSlotForTheNextClient() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-9", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-9a"));
        m.startOnce();
        assertFalse(m.hasCapacity());

        Queues.get(Queues.doneOcrQueueName("mgr-9"))
                .send(Message.createDoneOCR("http://a", "text", "app-9a"));
        m.startOnce();
        assertEquals(1, m.getServedClients());
        assertEquals(0, m.getActiveClients());
        assertTrue(m.hasCapacity());
        assertEquals(1, Queues.get(Queues.clientQueueName("app-9a")).receive(1, 1).size());

        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-9b"));
        m.startOnce();
        assertEquals(1, m.getActiveClients());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        assertEquals(0, m.getRejectedClients());
    }

    @Test
    public void emptyInputFreesItsSlotAtOnce() throws Exception {
        Manager m = new Manager("mgr-10", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(inputFile(), "app-10"));
        m.startOnce();
        assertEquals(1, m.getServedClients());
        assertTrue(m.hasCapacity());
    }

    @Test
    public void duplicateNewTaskIsOneEntryAndNotBusy() throws Exception {
        String in = inputFile("http://a", "http://b");
        Manager m = new Manager("mgr-11", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-11"));
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-11"));
        m.startOnce();
        assertEquals(1, m.getActiveClients());
        assertEquals(0, m.getRejectedClients());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        assertTrue(Queues.get(Queues.clientQueueName("app-11")).receive(1, 1).isEmpty());
    }

    @Test
    public void summaryFailureAnswersErrorAndStillFreesTheSlot() throws Exception {
        String in = inputFile();
        // A directory where the summary file must be written makes writeSummary fail.
        assertTrue(new File(in + ".summary").mkdir());
        Manager m = new Manager("mgr-12", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-12"));
        m.startOnce();

        List<Received> replies = Queues.get(Queues.clientQueueName("app-12")).receive(1, 1);
        assertEquals(1, replies.size());
        assertEquals(Message.MessageType.DONE_TASK, replies.get(0).getMessage().getType());
        assertTrue(replies.get(0).getMessage().isError(), "client must be told, not left hanging");
        assertNotNull(replies.get(0).getMessage().getOutputFileLocation(),
                "summary failures need a description distinct from a busy reply");
        assertTrue(replies.get(0).getMessage().getOutputFileLocation().startsWith("Summary failure:"));
        assertEquals(0, m.getServedClients());
        assertEquals(0, m.getActiveClients());
        assertTrue(m.hasCapacity());
        assertTrue(m.isRunning());
    }

    @Test
    public void instanceIdFromConstructorIsReportedBack() {
        Manager m = new Manager("i-12345", 2, 1);
        assertEquals("i-12345", m.getManagerID());
    }

    @Test
    public void workersNeededFormula() {
        assertEquals(1, Manager.workersNeeded(1, 5));
        assertEquals(1, Manager.workersNeeded(5, 5));
        assertEquals(2, Manager.workersNeeded(6, 5));
        assertEquals(0, Manager.workersNeeded(0, 5));
    }

    @Test
    public void nonDevReadUrlsReadsFromS3() {
        // Pre-register in-memory queues so the manager (and this test) use the in-memory
        // transport even though isDev() is false: only the S3 read/write paths are simulated.
        Queues.register(Queues.managerQueueName(), new InMemoryQueue());
        Queues.register(Queues.workerQueueName(), new InMemoryQueue());
        Queues.register(Queues.doneOcrQueueName("mgr-s3"), new InMemoryQueue());
        Queues.setDev(false);
        try {
            Manager m = new Manager("mgr-s3", 10, 1) {
                @Override
                protected List<String> readUrls(String location) {
                    assertEquals("assignment1/input-files/app-x-input.txt", location);
                    return java.util.Arrays.asList("http://a", "http://b");
                }
                @Override
                protected void writeSummary(ClientTask task, String summaryKey) {
                    assertEquals("assignment1/summaries/app-1.txt", summaryKey);
                }
            };
            Queues.get(Queues.managerQueueName())
                    .send(Message.createNewTask("assignment1/input-files/app-x-input.txt", "app-1"));
            m.startOnce();
            assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        } finally {
            Queues.setDev(true);
        }
    }

    @Test
    public void failedNewTaskRepliesWithErrorInsteadOfSilentlyDropping() throws Exception {
        Manager m = new Manager("mgr-err", 10, 1) {
            @Override
            protected List<String> readUrls(String location) throws IOException {
                throw new IOException("S3 unavailable");
            }
        };
        m.startOnce();
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask("in.txt", "app-err"));
        m.startOnce();
        List<Received> replies = Queues.get(Queues.clientQueueName("app-err")).receive(1, 1);
        assertEquals(1, replies.size());
        assertTrue(replies.get(0).getMessage().isError());
    }
}
