package utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqsQueueTest {
    @Test
    public void constructionDoesNotContactAws() {
        // No credentials here: the URL must be resolved lazily, on first use.
        Queue q = new SqsQueue("some_queue");
        assertTrue(q instanceof SqsQueue);
    }

    @Test
    public void chunksContainAtMostTenMessagesAndPreserveOrder() {
        List<Message> messages = new ArrayList<Message>();
        List<String> expectedUrls = new ArrayList<String>();
        for (int i = 0; i < 23; i++) {
            String url = "image-" + i;
            messages.add(Message.createNewImageTask(url, "app-1", "manager-1"));
            expectedUrls.add(url);
        }

        List<List<Message>> chunks = SqsQueue.chunkMessages(messages);

        assertEquals(Arrays.asList(10, 10, 3), chunkSizes(chunks));
        List<String> actualUrls = new ArrayList<String>();
        for (List<Message> chunk : chunks) {
            for (Message message : chunk) {
                actualUrls.add(message.getImageURL());
            }
        }
        assertEquals(expectedUrls, actualUrls);
    }

    @Test
    public void emptyMessageListProducesNoChunks() {
        assertTrue(SqsQueue.chunkMessages(new ArrayList<Message>()).isEmpty());
    }

    private static List<Integer> chunkSizes(List<List<Message>> chunks) {
        List<Integer> sizes = new ArrayList<Integer>();
        for (List<Message> chunk : chunks) {
            sizes.add(chunk.size());
        }
        return sizes;
    }
}
