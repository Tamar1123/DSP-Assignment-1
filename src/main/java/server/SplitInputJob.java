package server;

import utils.Message;
import utils.Queues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// Reads the input, sends image tasks in batches, drains finishes between batches.
final class SplitInputJob extends IoWorker.Job {
    private static final int IMAGE_TASK_BATCH = 10;
    private final ManagerSeams m;
    private final ClientTask task;

    SplitInputJob(ManagerSeams m, ClientTask task) {
        super(IoWorker.DISPATCH);
        this.m = m;
        this.task = task;
    }

    @Override
    public void run() {
        try {
            m.updateFreeSlots(); // advertise capacity at admission, before the URLs fan out
            List<String> urls = Collections.unmodifiableList(m.readUrls(task.inputFileLocation));
            task.urls = urls;
            for (String url : urls) {
                task.awaiting.computeIfAbsent(url, new java.util.function.Function<String, AtomicInteger>() {
                    @Override
                    public AtomicInteger apply(String k) {
                        return new AtomicInteger(0);
                    }
                }).incrementAndGet();
            }
            task.outstanding.set(urls.size());
            List<Message> batch = new ArrayList<Message>(IMAGE_TASK_BATCH);
            for (String url : urls) {
                batch.add(Message.createNewImageTask(url, task.localAppID, m.managerID()));
                if (batch.size() == IMAGE_TASK_BATCH) {
                    m.logDebug("sending " + batch.size() + " NEW_IMAGE_TASK to " + Queues.workerQueueName());
                    m.workerQueue().sendAll(batch);
                    batch = new ArrayList<Message>(IMAGE_TASK_BATCH);
                    m.runPendingFinishes();
                }
            }
            if (!batch.isEmpty()) {
                m.logDebug("sending " + batch.size() + " NEW_IMAGE_TASK to " + Queues.workerQueueName());
                m.workerQueue().sendAll(batch);
            }
            m.logDebug("admitted client " + task.localAppID + " with " + urls.size() + " images");
            if (urls.isEmpty()) {
                m.submit(new CompleteClientJob(m, task)); // empty input finishes at once
            }
            m.ensureWorkers(m.clients().totalOutstanding());
        } catch (Exception e) {
            m.failTask(task, "Input failure: " + Manager.describe(e));
        }
    }
}
