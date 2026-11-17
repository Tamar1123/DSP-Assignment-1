package server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerTest {
    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
    }


    @Test
    public void realImageOcrSmokeTest() throws Exception {
        Worker w = new Worker();
        // החלף בכתובת אמיתית מתוך inputExample.txt
        String sampleImageUrl = "https://www.cs.bgu.ac.il/~dsps211/Assignments/images/sample.png";
        String extractedText = w.identify(sampleImageUrl);
        System.out.println("Extracted text:\n" + extractedText);
        assertNotNull(extractedText);
    }

    @Test
    public void imageTaskIsAnsweredOnTheManagersQueue() {
        Worker w = new Worker();
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "mgr-1"));
        w.startOnce();
        List<Received> out = Queues.get(Queues.doneOcrQueueName("mgr-1")).receive(1, 1);
        assertEquals(1, out.size());
        assertEquals(Message.MessageType.DONE_OCR, out.get(0).getMessage().getType());
        assertEquals("http://img", out.get(0).getMessage().getImageURL());
        assertEquals("app-1", out.get(0).getMessage().getLocalAppID());
        assertFalse(out.get(0).getMessage().isError());
    }

    @Test
    public void taskIsDeletedAfterAnswering() {
        Worker w = new Worker();
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "mgr-1"));
        w.startOnce();
        assertTrue(Queues.get(Queues.workerQueueName()).receive(1, 0).isEmpty());
    }

    @Test
    public void failureStillRepliesWithAnError() {
        Worker w = new Worker() {
            @Override
            public String identify(String imageURL) {
                throw new IllegalStateException("boom");
            }
        };
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "mgr-1"));
        w.startOnce();
        List<Received> out = Queues.get(Queues.doneOcrQueueName("mgr-1")).receive(1, 1);
        assertEquals(1, out.size());
        assertTrue(out.get(0).getMessage().isError());
        assertEquals("http://img", out.get(0).getMessage().getImageURL());
    }

    @Test
    public void replyToAMissingManagerDoesNotKillTheWorker() {
        Worker w = new Worker();
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "gone"));
        w.startOnce();
        assertTrue(w.isRunning());
    }

    @Test
    public void unknownMessageTypeIsDropped() {
        Worker w = new Worker();
        Queues.get(Queues.workerQueueName()).send(Message.createNewTask("in.txt", "app-1"));
        w.startOnce();
        assertTrue(Queues.get(Queues.workerQueueName()).receive(1, 0).isEmpty());
    }
}
