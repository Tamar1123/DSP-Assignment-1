package utils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns the queue names used across the system and chooses the transport implementation.
 *
 * <p>Every component asks for a named queue through {@link #get(String)} and never constructs
 * a queue itself. In dev mode the queues live in this JVM ({@link InMemoryQueue}); in a real
 * run they are real SQS queues ({@link SqsQueue}).</p>
 */
public final class Queues {
    public static final String MANAGER_QUEUE = "manager_queue";
    public static final String WORKER_QUEUE = "worker_queue";

    /** SQS limits queue names to 80 characters. */
    public static final int MAX_NAME_LENGTH = 80;

    // ==== TRANSPORT SWITCH =====================================================
    // Dev: all components in one JVM, queues are ArrayBlockingQueue. Nothing talks to AWS.
    // Graded / real run: comment the DEV line, uncomment the AWS line, and make sure the
    // manager and workers run as separate processes (EC2). Nothing else changes.
    public static final boolean DEV = true;          // <-- in-memory
    // public static final boolean DEV = false;      // <-- SQS
    // ===========================================================================

    private static final ConcurrentHashMap<String, Queue> QUEUES =
            new ConcurrentHashMap<String, Queue>();

    private static volatile boolean dev = DEV;

    private Queues() {
    }

    public static String managerQueueName() {
        return MANAGER_QUEUE;
    }

    public static String workerQueueName() {
        return WORKER_QUEUE;
    }

    public static String clientQueueName(String localAppID) {
        return sanitize("client_", localAppID);
    }

    public static String doneOcrQueueName(String managerID) {
        return sanitize("done_ocr_", managerID);
    }

    /**
     * Returns the queue with this name for the current transport mode, creating it on first
     * use so that every caller in the JVM shares the same instance.
     */
    public static Queue get(String queueName) {
        return get(queueName, null);
    }

    /**
     * Returns the queue with this name. A {@code null} {@code dev} means "use the current
     * mode" ({@link #isDev()}); otherwise the given mode decides for this call.
     */
    public static Queue get(String queueName, Boolean dev) {
        final boolean devFlag = dev == null ? isDev() : dev.booleanValue();
        return QUEUES.computeIfAbsent(queueName, n -> devFlag ? new InMemoryQueue() : new SqsQueue(n));
    }

    /**
     * Creates the named queue now instead of waiting for the first send/receive.
     *
     * <p>In dev mode the in-memory queue only lives in this JVM, so this is just
     * {@link #get(String)}. In SQS mode {@link SqsQueue} resolves its URL lazily, so a caller
     * that must guarantee the queue exists before another process starts using it (for example
     * a client creating its reply queue before sending a request) has to ask for it here.
     * SQS {@code createQueue} is idempotent, so repeated calls are safe.</p>
     */
    public static void create(String queueName) {
        if (isDev()) {
            get(queueName);
        } else {
            AWS.getInstance().createQueue(queueName);
        }
    }

    public static void setDev(boolean dev) {
        Queues.dev = dev;
    }

    public static boolean isDev() {
        return dev;
    }

    /**
     * Test/dev seam only: forgets every queue created so far and restores the mode to
     * {@link #DEV}. Production code must not call this.
     */
    public static void reset() {
        QUEUES.clear();
        dev = DEV;
    }

    private static String sanitize(String prefix, String id) {
        String raw = id == null ? "" : id;
        String replacement = raw.replaceAll("[^A-Za-z0-9_-]", "_");
        String name = prefix + replacement;
        return name.length() > MAX_NAME_LENGTH ? name.substring(0, MAX_NAME_LENGTH) : name;
    }
}
