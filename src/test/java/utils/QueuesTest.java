package utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class QueuesTest {
    @Test
    public void fixedNames() {
        assertEquals("manager_queue", Queues.managerQueueName());
        assertEquals("worker_queue", Queues.workerQueueName());
    }

    @Test
    public void clientAndManagerNamesArePrefixed() {
        assertEquals("client_abc-123", Queues.clientQueueName("abc-123"));
        assertEquals("done_ocr_abc-123", Queues.doneOcrQueueName("abc-123"));
    }

    @Test
    public void namesAreSanitized() {
        assertEquals("client_a_b_c", Queues.clientQueueName("a.b/c"));
        assertEquals("done_ocr_a_b", Queues.doneOcrQueueName("a b"));
    }

    @Test
    public void namesAreTruncatedTo80() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) sb.append('x');
        assertEquals(80, Queues.clientQueueName(sb.toString()).length());
        assertEquals(80, Queues.doneOcrQueueName(sb.toString()).length());
    }

    @Test
    public void emptyIdStillProducesAValidName() {
        assertEquals("client_", Queues.clientQueueName(""));
        assertEquals("client_", Queues.clientQueueName(null));
    }

    @Test
    public void registerReplacesTheQueueGetReturns() {
        Queue custom = new InMemoryQueue();
        Queues.register("done_ocr_mgr-1", custom);
        assertSame(custom, Queues.get("done_ocr_mgr-1"));
    }

    @Test
    public void devModeReturnsInMemoryQueues() {
        Queues.reset();
        assertTrue(Queues.get("q1", Boolean.TRUE) instanceof InMemoryQueue);
        assertSame(Queues.get("q1", Boolean.TRUE), Queues.get("q1", Boolean.TRUE));
    }

    @Test
    public void currentModeIsUsedWhenDevIsNull() {
        Queues.reset();
        Queues.setDev(true);
        assertTrue(Queues.get("q2") instanceof InMemoryQueue);
        assertTrue(Queues.get("q2", null) instanceof InMemoryQueue);
    }

    @Test
    public void createMakesTheQueueAvailable() {
        Queues.reset();
        Queues.setDev(true);
        Queues.create("q-create");
        assertNotNull(Queues.get("q-create"));
        assertTrue(Queues.get("q-create") instanceof InMemoryQueue);
    }

    @Test
    public void awsModeReturnsSqsQueuesWithoutContactingAws() {
        Queues.reset();
        assertTrue(Queues.get("q3", Boolean.FALSE) instanceof SqsQueue);
    }
}
