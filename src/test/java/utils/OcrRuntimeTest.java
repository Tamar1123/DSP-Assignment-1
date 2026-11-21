package utils;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

// The worker must be able to OCR with nothing installed on the machine: the engine, its shared
// library closure and the trained data all come from the classpath.
public class OcrRuntimeTest {

    @Test
    public void bundledEngineAndTrainedDataAreShipped() {
        for (String name : OcrRuntime.LIBRARIES) {
            assertTrue(OcrRuntime.class.getClassLoader()
                            .getResource(OcrRuntime.NATIVE_RESOURCE_DIR + "/" + name) != null,
                    "missing bundled library: " + name);
        }
    }

    @Test
    public void ensureReadyUnpacksNativeLibrariesAndTrainedDataOffTheJar() {
        Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase().contains("linux")
                && System.getProperty("os.arch").equals("amd64"),
                "bundled native libraries are linux-x86-64 only");
        String tessdata = OcrRuntime.ensureReady();

        File eng = new File(tessdata, "eng.traineddata");
        assertTrue(eng.isFile(), "eng.traineddata not extracted to " + tessdata);
        assertTrue(eng.length() > 1000000, "eng.traineddata looks truncated: " + eng.length());

        File libtesseract = new File(new File(System.getProperty("java.io.tmpdir"), "dsp-ocr-native"),
                "libtesseract.so");
        assertTrue(libtesseract.isFile(), "libtesseract.so not extracted to " + libtesseract);
        assertTrue(libtesseract.length() > 1000000, "libtesseract.so looks truncated");
    }
}
