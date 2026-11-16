package utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqsQueueTest {
    @Test
    public void constructionDoesNotContactAws() {
        // No credentials here: the URL must be resolved lazily, on first use.
        Queue q = new SqsQueue("some_queue");
        assertTrue(q instanceof SqsQueue);
    }
}
