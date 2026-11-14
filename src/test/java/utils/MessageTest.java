package utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MessageTest {
    @Test
    public void newImageTaskCarriesManagerId() {
        Message m = Message.createNewImageTask("http://img", "app-1", "mgr-1");
        assertEquals(Message.MessageType.NEW_IMAGE_TASK, m.getType());
        assertEquals("http://img", m.getImageURL());
        assertEquals("app-1", m.getLocalAppID());
        assertEquals("mgr-1", m.getManagerID());
    }

    @Test
    public void jsonRoundTripKeepsEveryField() {
        Message m = Message.createNewImageTask("http://img", "app-1", "mgr-1");
        Message back = Message.fromJson(m.toJson());
        assertEquals(Message.MessageType.NEW_IMAGE_TASK, back.getType());
        assertEquals("http://img", back.getImageURL());
        assertEquals("app-1", back.getLocalAppID());
        assertEquals("mgr-1", back.getManagerID());
    }

    @Test
    public void doneOcrRoundTrip() {
        Message m = Message.createDoneOCR("http://img", "hello", "app-1");
        Message back = Message.fromJson(m.toJson());
        assertEquals(Message.MessageType.DONE_OCR, back.getType());
        assertEquals("hello", back.getIdentifiedText());
        assertEquals("app-1", back.getLocalAppID());
    }
}
