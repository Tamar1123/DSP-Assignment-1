package server;
import com.asprise.ocr.Ocr;
import java.io.File;


public class Worker {

    public Worker() {

    }

    public static String processOne(String inputFileName) {
        Ocr.setUp(); // one time setup
        Ocr ocr = new Ocr(); // create a new OCR engine
        ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
        String s = ocr.recognize(new File[] {new File(inputFileName)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
        ocr.stopEngine();
        return s;
    }
}
