package server;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// One admitted client's progress: its images, collected texts and whether it was answered.
final class ClientTask {
    final String localAppID;
    final String inputFileLocation;
    // First text per URL wins.
    final ConcurrentHashMap<String, String> texts = new ConcurrentHashMap<String, String>();
    // Image tasks not yet answered.
    final AtomicInteger outstanding = new AtomicInteger();
    // Answers the client exactly once.
    final AtomicBoolean done = new AtomicBoolean(false);
    // Input order, set before the first image task is sent.
    volatile List<String> urls = Collections.emptyList();
    // Remaining expected OCR results per URL; populated before fan-out.
    final ConcurrentHashMap<String, AtomicInteger> awaiting =
            new ConcurrentHashMap<String, AtomicInteger>();

    ClientTask(String localAppID, String inputFileLocation) {
        this.localAppID = localAppID;
        this.inputFileLocation = inputFileLocation;
    }
}
