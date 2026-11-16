package server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.io.File;
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
    }

    @Test
    public void terminateAcksServedClientsAndStops() throws Exception {
        String in = inputFile();
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
}
