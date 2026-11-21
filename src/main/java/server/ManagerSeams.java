package server;

import utils.Queue;
import java.io.IOException;
import java.util.List;

// What the IoWorker jobs need from the manager; keeps the jobs out of Manager.
interface ManagerSeams {
    String managerID();
    Queue workerQueue();
    ClientRegistry clients();
    void submit(IoWorker.Job job);
    void runPendingFinishes();
    void updateFreeSlots();
    List<String> readUrls(String inputFileLocation) throws IOException;
    void writeSummary(ClientTask task, String summaryLocation) throws IOException;
    void ensureWorkers(int outstanding);
    void failTask(ClientTask task, String description);
    void logDebug(String message);
}
