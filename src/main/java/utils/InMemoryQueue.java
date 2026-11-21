package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-memory {@link Queue} used in dev mode. It mimics the SQS shape (receive/delete/release)
 * without any network access.
 */
public class InMemoryQueue implements Queue {
    static final int QUEUE_CAPACITY = 1000;

    private final BlockingQueue<Received> queue = new ArrayBlockingQueue<Received>(QUEUE_CAPACITY);

    public InMemoryQueue() {
    }

    @Override
    public void send(Message msg) {
        try {
            queue.put(new Received(msg, UUID.randomUUID().toString()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while sending to in-memory queue", e);
        }
    }

    @Override
    public List<Received> receive(int max, int waitSeconds) {
        long deadline = System.currentTimeMillis() + waitSeconds * 1000L;
        List<Received> out = new ArrayList<Received>();
        while (out.size() < max) {
            long remaining = deadline - System.currentTimeMillis();
            Received r;
            try {
                r = remaining > 0 ? queue.poll(remaining, TimeUnit.MILLISECONDS) : queue.poll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (r == null) {
                break;
            }
            out.add(r);
        }
        return out;
    }

    @Override
    public void delete(Received received) {
        // No-op in dev: the message already left the queue on receive, so there is nothing
        // left to delete. Real SQS needs the receipt handle here.
    }

    @Override
    public void release(Received received) {
        // Dev loses ordering on release (put appends to the tail); acceptable in-memory.
        try {
            // Block rather than offer: at capacity an offer would silently drop the message.
            queue.put(received);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while releasing to in-memory queue", e);
        }
    }

    @Override
    public void release(Received received, int delaySeconds) {
        // Dev has no visibility clock: the delay is ignored and the message is back at once.
        release(received);
    }
}
