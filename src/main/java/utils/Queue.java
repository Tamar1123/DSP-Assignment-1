package utils;

import java.util.List;

/**
 * Transport contract shared by the in-memory dev queue and the real SQS queue.
 */
public interface Queue {
    void send(Message msg);

    /**
     * Sends several messages. The default sends them one by one; a transport that
     * supports batching may override this. Order within the list is preserved.
     */
    default void sendAll(List<Message> messages) {
        for (Message msg : messages) {
            send(msg);
        }
    }

    /**
     * Receives up to {@code max} messages, waiting at most {@code waitSeconds} for them.
     * Returns an empty list (never {@code null}) when the timeout expires with no message.
     */
    List<Received> receive(int max, int waitSeconds);

    void delete(Received received);

    void release(Received received);

    /**
     * Makes the message visible again after {@code delaySeconds}, so a consumer that cannot
     * take it now (for example, a full manager) does not get it back immediately. The default
     * ignores the delay and delegates to {@link #release(Received)}; transports that support a
     * visibility delay override this.
     */
    default void release(Received received, int delaySeconds) {
        release(received);
    }
}
