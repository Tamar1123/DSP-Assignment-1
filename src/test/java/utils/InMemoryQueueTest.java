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
    public void sendAllEnqueuesMessagesInInputOrder() {
        Queue q = new InMemoryQueue();
        q.sendAll(java.util.Arrays.asList(
                Message.createNewImageTask("a", "app-1", "manager-1"),
                Message.createNewImageTask("b", "app-1", "manager-1"),
                Message.createNewImageTask("c", "app-1", "manager-1")));

        List<Received> got = q.receive(3, 0);
        assertEquals(3, got.size());
        assertEquals("a", got.get(0).getMessage().getImageURL());
        assertEquals("b", got.get(1).getMessage().getImageURL());
        assertEquals("c", got.get(2).getMessage().getImageURL());
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
    public void releaseAtCapacityDoesNotDropTheMessage() throws InterruptedException {
        final Queue q = new InMemoryQueue();
        for (int i = 0; i < InMemoryQueue.QUEUE_CAPACITY; i++) {
            q.send(Message.createNewTask("url-" + i, "app-1"));
        }
        final Received released = q.receive(1, 0).get(0);
        // Refill the freed slot so the queue is genuinely full when release runs.
        q.send(Message.createNewTask("filler", "app-1"));

        Thread releaser = new Thread(new Runnable() {
            @Override
            public void run() {
                q.release(released);
            }
        });
        releaser.start();

        // A blocking put cannot finish while the queue is full.
        long deadline = System.currentTimeMillis() + 5000;
        while (releaser.getState() != Thread.State.WAITING && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        // Free a slot so the release can complete instead of dropping the message.
        assertEquals(1, q.receive(1, 0).size());
        releaser.join(5000);
        assertFalse(releaser.isAlive(), "release must not block once a slot is free");

        boolean found = false;
        for (Received r : q.receive(InMemoryQueue.QUEUE_CAPACITY, 0)) {
            if (r.getMessage().getInputFileLocation().equals(released.getMessage().getInputFileLocation())) {
                found = true;
            }
        }
        assertTrue(found, "the released message must stay in the queue");
    }

    @Test
    public void releaseWithDelayPutsTheMessageBackAtOnce() {
        // The dev queue ignores the delay: the message is visible again immediately.
        Queue q = new InMemoryQueue();
        q.send(Message.createNewTask("in.txt", "app-d"));
        Received r = q.receive(1, 0).get(0);
        q.release(r, 5);
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
