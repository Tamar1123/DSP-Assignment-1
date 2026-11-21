package utils;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

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
    public void sendAll(List<Message> messages) {
        int offset = 0;
        for (List<Message> chunk : chunkMessages(messages)) {
            List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
            for (int i = 0; i < chunk.size(); i++) {
                entries.add(SendMessageBatchRequestEntry.builder()
                        .id("entry-" + (offset + i))
                        .messageBody(chunk.get(i).toJson())
                        .build());
            }

            SendMessageBatchResponse response = sqs().sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(url())
                    .entries(entries)
                    .build());
            if (!response.failed().isEmpty()) {
                List<String> failedIds = new ArrayList<String>();
                for (software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry failed : response.failed()) {
                    failedIds.add(failed.id());
                }
                throw new RuntimeException("Failed to send SQS batch entries: " + failedIds);
            }
            offset += chunk.size();
        }
    }

    /** Splits messages into ordered batches accepted by SQS sendMessageBatch. */
    static List<List<Message>> chunkMessages(List<Message> messages) {
        List<List<Message>> chunks = new ArrayList<List<Message>>();
        for (int start = 0; start < messages.size(); start += 10) {
            int end = Math.min(start + 10, messages.size());
            chunks.add(new ArrayList<Message>(messages.subList(start, end)));
        }
        return chunks;
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
                    try {
                        AWS.getInstance().getSqs().setQueueAttributes(
                                software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest.builder()
                                        .queueUrl(resolved)
                                        .attributes(java.util.Collections.singletonMap(
                                                software.amazon.awssdk.services.sqs.model.QueueAttributeName
                                                        .VISIBILITY_TIMEOUT, "300"))
                                        .build());
                    } catch (Exception e) {
                        System.err.println("[WARN] could not set visibility timeout on "
                                + queueName + ": " + e.getMessage());
                    }
                    queueUrl = resolved;
                }
            }
        }
        return resolved;
    }
}
