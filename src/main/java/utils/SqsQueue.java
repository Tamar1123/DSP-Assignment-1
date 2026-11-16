package utils;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link Queue} backed by a real AWS SQS queue.
 *
 * The queue URL is resolved lazily on first use, so constructing an instance (for example,
 * from {@code Queues.get(name, false)}) never contacts AWS and needs no credentials.
 */
public class SqsQueue implements Queue {
    private final String queueName;
    private volatile String queueUrl;

    public SqsQueue(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void send(Message msg) {
        sqs().sendMessage(b -> b.queueUrl(url()).messageBody(msg.toJson()));
    }

    @Override
    public List<Received> receive(int max, int waitSeconds) {
        ReceiveMessageResponse response = sqs().receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(url())
                .maxNumberOfMessages(max)
                .waitTimeSeconds(waitSeconds)
                .build());

        List<Received> out = new ArrayList<Received>();
        for (software.amazon.awssdk.services.sqs.model.Message raw : response.messages()) {
            Message msg;
            try {
                msg = Message.fromJson(raw.body());
            } catch (RuntimeException e) {
                // A malformed body must not abort the batch or keep coming back: drop it.
                // No debug output here: App.debug is package-private in client until a later
                // task widens it, and this path cannot be exercised offline without AWS.
                deleteRaw(raw);
                continue;
            }
            if (msg == null) {
                // JSON "null" parses to null: also poison, delete it rather than pass it on.
                deleteRaw(raw);
                continue;
            }
            out.add(new Received(msg, raw.receiptHandle()));
        }
        return out;
    }

    @Override
    public void delete(Received received) {
        sqs().deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(url())
                .receiptHandle(received.getReceiptHandle())
                .build());
    }

    @Override
    public void release(Received received) {
        sqs().changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(url())
                .receiptHandle(received.getReceiptHandle())
                .visibilityTimeout(0)
                .build());
    }

    private SqsClient sqs() {
        return AWS.getInstance().getSqs();
    }

    private void deleteRaw(software.amazon.awssdk.services.sqs.model.Message raw) {
        sqs().deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(url())
                .receiptHandle(raw.receiptHandle())
                .build());
    }

    /** Resolves the queue URL once, thread-safely, on first use. */
    private String url() {
        String resolved = queueUrl;
        if (resolved == null) {
            synchronized (this) {
                resolved = queueUrl;
                if (resolved == null) {
                    resolved = AWS.getInstance().createQueue(queueName);
                    queueUrl = resolved;
                }
            }
        }
        return resolved;
    }
}
