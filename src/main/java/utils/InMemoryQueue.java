package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-memory {@link Queue} used in dev mode. It mimics the SQS shape (receive/delete/release)
 * without any network access.
 */
public class InMemoryQueue implements Queue {
    static final int QUEUE_CAPACITY = 1000;

    private final BlockingQueue<Received> queue = new ArrayBlockingQueue<Received>(QUEUE_CAPACITY);
    /**
     * Messages given back with {@link #release(Received)}. Kept outside the bounded queue so a
     * release can never block the caller (a full manager moves on; a blocked router would stall
     * every client) and never drops the message. FIFO, like SQS.
     */
    private final ConcurrentLinkedQueue<Received> released = new ConcurrentLinkedQueue<Received>();

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
        // Released messages first: they are older than whatever arrived after them.
        Received requeued;
        while (out.size() < max && (requeued = released.poll()) != null) {
            out.add(requeued);
        }
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
        // Never blocks and never drops: the unbounded released FIFO is independent of the
        // bounded send path, so a release at capacity cannot stall the receiving thread.
        released.add(received);
    }

    @Override
    public void release(Received received, int delaySeconds) {
        // Dev has no visibility clock: the delay is ignored and the message is back at once.
        release(received);
    }
}
