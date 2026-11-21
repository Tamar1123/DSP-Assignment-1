package utils;

import net.sourceforge.tess4j.util.LoadLibs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

// Prepares the OCR engine without anything installed on the node.
// The native Tesseract/Leptonica libraries and the language data ship inside the jar (see
// src/main/resources/ocr-native/linux-x86-64 and the tess4j jar's tessdata). On first use they
// are copied to a temp directory, the shared library closure is loaded in dependency order, and
// JNA is pointed at the same directory. Afterwards Tess4J loads libtesseract.so like a system
// installation. Damaged or unsupported resources (for example a non-Linux machine) do not fail
// here: the caller then falls back to a system installation, exactly as before.
public final class OcrRuntime {

    // Classpath directory holding the bundled shared objects.
    static final String NATIVE_RESOURCE_DIR = "ocr-native/linux-x86-64";

    // Load order matters: glibc resolves a dependency only if its soname is already loaded.
    // Order follows the DT_NEEDED graph (leaf libraries first, libtesseract last).
    static final String[] LIBRARIES = {
            "libgif.so.7",
            "libopenjp2.so.7",
            "libwebp.so.7",
            "libwebpmux.so.3",
            "libwebpdemux.so.2",
            "libpng16.so.16",
            "libjpeg.so.8",
            "libjbig.so.0",
            "libdeflate.so.0",
            "libtiff.so.5",
            "liblept.so.5",
            "libtesseract.so"
    };

    private static boolean initialized;
    private static String tessdataPath;

    private OcrRuntime() {
    }

    // Extracts and loads the bundled engine once and returns the directory that holds
    // eng.traineddata for Tesseract.setDatapath. TESSDATA_PREFIX still wins, so a machine with
    // its own trained data can override the bundled copy.
    public static synchronized String ensureReady() {
        if (initialized) {
            return tessdataPath;
        }

        File nativeDir = extractBundledLibs();
        if (nativeDir != null) {
            try {
                loadLibraries(nativeDir);
                System.setProperty("jna.library.path", nativeDir.getAbsolutePath());
            } catch (RuntimeException | LinkageError e) {
                // For example an image whose glibc is older than the bundled binaries'.
                // Leave JNA alone so a system Tesseract installation is used instead.
                System.err.println("[OCR] bundled native libraries unusable (" + e.getMessage()
                        + "); falling back to the system Tesseract installation");
            }
        }
        tessdataPath = resolveTessdata();
        initialized = true;
        return tessdataPath;
    }

    private static String resolveTessdata() {
        String fromEnv = System.getenv("TESSDATA_PREFIX");
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return new File(fromEnv.trim()).getAbsolutePath();
        }
        // tess4j (and every other jar that puts tessdata on the classpath) is unpacked here.
        File extracted = LoadLibs.extractTessResources("tessdata");
        return extracted.getAbsolutePath();
    }

    // Returns the directory with the bundled libraries, or null when none are bundled.
    private static File extractBundledLibs() {
        File dir = new File(System.getProperty("java.io.tmpdir"), "dsp-ocr-native");
        boolean any = false;
        for (String name : LIBRARIES) {
            if (!extractResource(NATIVE_RESOURCE_DIR + "/" + name, new File(dir, name))) {
                return null;
            }
            any = true;
        }
        return any ? dir : null;
    }

    private static void loadLibraries(File dir) {
        for (String name : LIBRARIES) {
            File lib = new File(dir, name);
            try {
                System.load(lib.getAbsolutePath());
            } catch (UnsatisfiedLinkError e) {
                // Already loaded by another class loader or an earlier call: fine.
                if (!isAlreadyLoaded(e)) {
                    throw new IllegalStateException("could not load bundled " + name, e);
                }
            }
        }
    }

    private static boolean isAlreadyLoaded(UnsatisfiedLinkError e) {
        String message = e.getMessage();
        return message != null && message.contains("already loaded in another classloader");
    }

    // Copies a classpath resource to target; keeps an existing identical file.
    private static boolean extractResource(String resource, File target) {
        InputStream in = OcrRuntime.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            return false;
        }
        File tmp = new File(target.getPath() + ".tmp");
        try {
            File dir = target.getParentFile();
            if (!dir.isDirectory() && !dir.mkdirs() && !dir.isDirectory()) {
                return false;
            }
            OutputStream out = new FileOutputStream(tmp);
            try {
                byte[] buffer = new byte[65536];
                int read;
                while ((read = in.read(buffer)) > 0) {
                    out.write(buffer, 0, read);
                }
            } finally {
                out.close();
            }
            // Extracted by an earlier worker on this node, or by this jar before: reuse it.
            if (target.isFile() && target.length() == tmp.length()) {
                tmp.delete();
                return true;
            }
            try {
                Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                return false;
            }
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            tmp.delete();
            try {
                in.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }
}
