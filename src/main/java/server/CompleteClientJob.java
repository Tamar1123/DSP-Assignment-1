package server;

import utils.AWS;
import utils.Message;
import utils.Queues;

// Answers once: summary, DONE_TASK, remove the task, free the slot.
// Answers once: summary, DONE_TASK, remove the task, free the slot.
final class CompleteClientJob extends IoWorker.Job {
    private final ManagerSeams m;
    private final ClientTask task;

    CompleteClientJob(ManagerSeams m, ClientTask task) {
        super(IoWorker.FINISH);
        this.m = m;
        this.task = task;
    }

    @Override
    public void run() {
        if (!task.done.compareAndSet(false, true)) {
            return; // answered once
        }
        String summaryLocation = Queues.isDev()
                ? task.inputFileLocation + ".summary"
                : AWS.S3_PREFIX + "summaries/" + task.localAppID + ".txt";
        try {
            m.writeSummary(task, summaryLocation);
            m.logDebug("summary written at " + summaryLocation + "; sending DONE_TASK to "
                    + Queues.clientQueueName(task.localAppID));
            Queues.get(Queues.clientQueueName(task.localAppID))
                    .send(Message.createDoneTask(summaryLocation, task.localAppID));
            m.clients().markServed();
            m.logDebug("finished client " + task.localAppID + " (served " + m.clients().served()
                    + ", free slots " + (m.clients().freeSlots()) + ")");
        } catch (Exception e) {
            // The summary could not be written or sent: tell the client so it does not hang.
            System.err.println("[MANAGER] could not finish client " + task.localAppID
                    + ": " + Manager.describe(e));
            try {
                Message failed = Message.createDoneTask("Summary failure: " + Manager.describe(e),
                        task.localAppID);
                failed.setError(true);
                Queues.get(Queues.clientQueueName(task.localAppID)).send(failed);
            } catch (Exception sendFailure) {
                System.err.println("[MANAGER] could not notify client " + task.localAppID
                        + " of failure: " + sendFailure.getMessage());
            }
        } finally {
            // Free the slot last, and always: a failed summary must not leak a client slot.
            m.clients().remove(task);
            m.updateFreeSlots();
        }
    }
}
