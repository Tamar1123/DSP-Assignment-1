package utils;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Message {
    public enum MessageType {
        NEW_TASK,
        NEW_IMAGE_TASK,
        DONE_OCR,
        DONE_TASK,
        TERMINATE
    }

    private MessageType type;
    private String inputFileLocation;
    private String imageURL;
    private String identifiedText;
    private String outputFileLocation;
    private String localAppID;
    private boolean isError;

    public Message() {}

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Message createNewTask(String inputFileLocation, String localAppID) {
        Message msg = new Message();
        msg.type = MessageType.NEW_TASK;
        msg.inputFileLocation = inputFileLocation;
        msg.localAppID = localAppID;
        return msg;
    }

    
    public static Message createNewImageTask(String imageURL, String localAppID) {
        Message msg = new Message();
        msg.type = MessageType.NEW_IMAGE_TASK;
        msg.imageURL = imageURL;
        msg.localAppID = localAppID;
        return msg;
    }

    
    public static Message createDoneOCR(String imageURL, String identifiedText, String localAppID) {
        Message msg = new Message();
        msg.type = MessageType.DONE_OCR;
        msg.imageURL = imageURL;
        msg.identifiedText = identifiedText;
        msg.localAppID = localAppID;
        return msg;
    }

    
    public static Message createDoneTask(String outputFileLocation, String localAppID) {
        Message msg = new Message();
        msg.type = MessageType.DONE_TASK;
        msg.outputFileLocation = outputFileLocation;
        msg.localAppID = localAppID;
        return msg;
    }

    
    public static Message createTerminate(String localAppID) {
        Message msg = new Message();
        msg.type = MessageType.TERMINATE;
        msg.localAppID = localAppID;
        return msg;
    }

    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert Message to JSON", e);
        }
    }

    public static Message fromJson(String json) {
        try {
            return objectMapper.readValue(json, Message.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON to Message", e);
        }
    }

    public MessageType getType() { return type; }
    public void setType(MessageType type) { this.type = type; }

    public String getLocalAppID() { return localAppID; }
    public void setLocalAppID(String localAppID) { this.localAppID = localAppID; }

    public String getInputFileLocation() { return inputFileLocation; }
    public void setInputFileLocation(String inputFileLocation) { this.inputFileLocation = inputFileLocation; }

    public String getImageURL() { return imageURL; }
    public void setImageURL(String imageURL) { this.imageURL = imageURL; }

    public String getIdentifiedText() { return identifiedText; }
    public void setIdentifiedText(String identifiedText) { this.identifiedText = identifiedText; }

    public String getOutputFileLocation() { return outputFileLocation; }
    public void setOutputFileLocation(String outputFileLocation) { this.outputFileLocation = outputFileLocation; }

    public boolean isError() { return isError; }
    public void setError(boolean error) { isError = error; }
}
