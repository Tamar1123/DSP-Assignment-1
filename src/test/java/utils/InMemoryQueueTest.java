package utils;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class InMemoryQueueTest {
    @Test
    public void sendThenReceiveReturnsTheMessage() {
        Queue q = new InMemoryQueue();
        q.send(Message.createNewTask("in.txt", "app-1"));
        List<Received> got = q.receive(1, 0);
        assertEquals(1, got.size());
        assertEquals(Message.MessageType.NEW_TASK, got.get(0).getMessage().getType());
        assertEquals("in.txt", got.get(0).getMessage().getInputFileLocation());
        assertNotNull(got.get(0).getReceiptHandle());
    }

    @Test
    public void receiveOnEmptyQueueReturnsEmptyListAndDoesNotBlock() {
        Queue q = new InMemoryQueue();
        long start = System.currentTimeMillis();
        List<Received> got = q.receive(10, 1);
        assertTrue(got.isEmpty());
        assertTrue(System.currentTimeMillis() - start < 3000);
    }

    @Test
    public void deleteRemovesTheMessagePermanently() {
        Queue q = new InMemoryQueue();
        q.send(Message.createNewTask("in.txt", "app-1"));
        Received r = q.receive(1, 0).get(0);
        q.delete(r);
        assertTrue(q.receive(1, 0).isEmpty());
    }

    @Test
    public void releaseMakesTheMessageVisibleAgain() {
        Queue q = new InMemoryQueue();
        q.send(Message.createNewTask("in.txt", "app-1"));
        Received r = q.receive(1, 0).get(0);
        q.release(r);
        List<Received> again = q.receive(1, 0);
        assertEquals(1, again.size());
        assertEquals("in.txt", again.get(0).getMessage().getInputFileLocation());
    }

    @Test
    public void receiveReturnsAtMostMax() {
        Queue q = new InMemoryQueue();
        q.send(Message.createNewTask("a", "app-1"));
        q.send(Message.createNewTask("b", "app-1"));
        q.send(Message.createNewTask("c", "app-1"));
        assertEquals(2, q.receive(2, 0).size());
        assertEquals(1, q.receive(2, 0).size());
    }
}
