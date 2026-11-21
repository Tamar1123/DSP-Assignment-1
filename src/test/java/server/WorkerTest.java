package server;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerTest {
    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
    }


    @org.junit.jupiter.api.Disabled("requires network; run manually")
    @Test
    public void realImageOcrSmokeTest() throws Exception {
        Worker w = new Worker();
        // החלף בכתובת אמיתית מתוך inputExample.txt
        String sampleImageUrl = "https://www.cs.bgu.ac.il/~dsps211/Assignments/images/sample.png";
        String extractedText = w.identify(sampleImageUrl);
        System.out.println("Extracted text:\n" + extractedText);
        assertNotNull(extractedText);
    }

    /**
     * Exercises the real Tess4J path without the network. Skips when the native Tesseract
     * library/trained data is not installed on this machine; EC2 nodes install it at boot.
     */
@Test
    public void tess4jReadsAGeneratedImage() throws Exception {
        File png = File.createTempFile("ocr-check-", ".png");
        png.deleteOnExit();

        String text;
        try {
            BufferedImage img = new BufferedImage(600, 150, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = img.createGraphics();
            g.setColor(Color.WHITE);
            g.fillRect(0, 0, 600, 150);
            g.setColor(Color.BLACK);
            g.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 48));
            g.drawString("HELLO 12345", 30, 95);
            g.dispose();
            ImageIO.write(img, "png", png);

            text = Worker.processOne(png.getAbsolutePath());
        } catch (RuntimeException | LinkageError e) {
            Assumptions.assumeTrue(false, "native graphics/Tesseract unavailable: " + e.getMessage());
            return;
        }
        assertTrue(text.contains("HELLO"), "OCR text was: " + text);
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
        assertTrue(Queues.get(Queues.workerQueueName()).receive(1, 0).isEmpty(),
                "an OCR exception is a completed error result, so the task is deleted");
    }

    @Test
    public void failedReplyReleasesTheTaskAndKeepsTheWorkerAlive() {
        Worker w = new Worker();
        Queues.register(Queues.doneOcrQueueName("mgr-1"), new FailingSendQueue());
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "mgr-1"));
        w.startOnce();

        List<Received> again = Queues.get(Queues.workerQueueName()).receive(1, 0);
        assertEquals(1, again.size(), "a failed reply must release, not delete, the task");
        assertEquals("http://img", again.get(0).getMessage().getImageURL());
        assertTrue(w.isRunning());
    }

    @Test
    public void releasedTaskReappearsOnTheQueue() {
        Queues.get(Queues.workerQueueName())
                .send(Message.createNewImageTask("http://img", "app-1", "mgr-1"));
        // A worker that receives but never deletes (simulates death before reply).
        List<Received> batch = Queues.get(Queues.workerQueueName()).receive(1, 1);
        assertEquals(1, batch.size());
        // Release: the message becomes visible again (dev: re-enqueued at tail).
        Queues.get(Queues.workerQueueName()).release(batch.get(0));
        List<Received> again = Queues.get(Queues.workerQueueName()).receive(1, 1);
        assertEquals(1, again.size());
        assertEquals("http://img", again.get(0).getMessage().getImageURL());
    }

    @Test
    public void unknownMessageTypeIsDropped() {
        Worker w = new Worker();
        Queues.get(Queues.workerQueueName()).send(Message.createNewTask("in.txt", "app-1"));
        w.startOnce();
        assertTrue(Queues.get(Queues.workerQueueName()).receive(1, 0).isEmpty());
    }

    private static final class FailingSendQueue implements Queue {
        public void send(Message msg) {
            throw new IllegalStateException("no manager queue");
        }

        public List<Received> receive(int max, int waitSeconds) {
            return new java.util.ArrayList<Received>();
        }

        public void delete(Received received) {
        }

        public void release(Received received) {
        }
    }
}
