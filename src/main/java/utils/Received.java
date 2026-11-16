package utils;

/**
 * A message received from a {@link Queue}, together with the receipt handle needed to
 * delete or release it.
 */
public final class Received {
    private final Message message;
    private final String receiptHandle;

    public Received(Message message, String receiptHandle) {
        this.message = message;
        this.receiptHandle = receiptHandle;
    }

    public Message getMessage() {
        return message;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }
}
