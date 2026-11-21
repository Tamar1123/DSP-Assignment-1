package server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ManagerTest {
    @BeforeEach
    public void setUp() {
        Queues.reset();
        Queues.setDev(true);
    }

    private String inputFile(String... urls) throws Exception {
        File f = File.createTempFile("input", ".txt");
        f.deleteOnExit();
        PrintWriter w = new PrintWriter(f);
        for (String u : urls) {
            w.println(u);
        }
        w.close();
        return f.getAbsolutePath();
    }

    @Test
    public void newTaskProducesOneImageTaskPerLine() throws Exception {
        String in = inputFile("http://a", "http://b");
        Manager m = new Manager("mgr-1");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-1"));
        m.startOnce();
        List<Received> tasks = Queues.get(Queues.workerQueueName()).receive(10, 1);
        assertEquals(2, tasks.size());
        assertEquals("mgr-1", tasks.get(0).getMessage().getManagerID());
        assertEquals("app-1", tasks.get(0).getMessage().getLocalAppID());
    }

    @Test
    public void emptyInputAnswersImmediately() throws Exception {
        String in = inputFile();
        Manager m = new Manager("mgr-2");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-2"));
        m.startOnce();
        List<Received> done = Queues.get(Queues.clientQueueName("app-2")).receive(1, 1);
        assertEquals(1, done.size());
        assertEquals(Message.MessageType.DONE_TASK, done.get(0).getMessage().getType());
    }

    @Test
    public void duplicateUrlsAreTwoTasks() throws Exception {
        String in = inputFile("http://a", "http://a");
        Manager m = new Manager("mgr-3");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-3"));
        m.startOnce();
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        for (int i = 0; i < 2; i++) {
            Queues.get(Queues.doneOcrQueueName("mgr-3"))
                    .send(Message.createDoneOCR("http://a", "text" + i, "app-3"));
        }
        m.startOnce();
        assertEquals(1, Queues.get(Queues.clientQueueName("app-3")).receive(1, 1).size());
    }

    @Test
    public void doneOcrFromUnknownClientIsIgnored() {
        Manager m = new Manager("mgr-4");
        Queues.get(Queues.doneOcrQueueName("mgr-4"))
                .send(Message.createDoneOCR("http://a", "t", "nobody"));
        m.startOnce();
        assertTrue(Queues.get(Queues.clientQueueName("nobody")).receive(1, 1).isEmpty());
        assertEquals(0, m.getServedClients());
        assertEquals(0, m.getActiveClients());
    }

    @Test
    public void terminateDrainsOutstandingWorkBeforeStopping() throws Exception {
        // app-5 still has one image outstanding: the manager must not stop or ack yet.
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-5");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-5"));
        m.startOnce();

        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-5"));
        m.startOnce();
        assertTrue(m.isRunning(), "must wait for the outstanding image");
        assertTrue(Queues.get(Queues.clientQueueName("app-5")).receive(10, 1).isEmpty());

        Queues.get(Queues.doneOcrQueueName("mgr-5"))
                .send(Message.createDoneOCR("http://a", "text", "app-5"));
        m.startOnce();
        m.startOnce();
        List<Received> replies = Queues.get(Queues.clientQueueName("app-5")).receive(10, 1);
        assertTrue(replies.stream().anyMatch(r -> r.getMessage().getType() == Message.MessageType.DONE_TASK));
        assertTrue(replies.stream().anyMatch(r -> r.getMessage().getType() == Message.MessageType.TERMINATE));
        assertFalse(m.isRunning());
    }

    @Test
    public void terminateAcksClientThatAlreadyFinished() throws Exception {
        // Bug 2: finish() removes the client, so handleTerminate used to find nobody to ack.
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-12");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-12"));
        m.startOnce();
        Queues.get(Queues.doneOcrQueueName("mgr-12"))
                .send(Message.createDoneOCR("http://a", "text", "app-12"));
        m.startOnce();
        assertEquals(0, m.getActiveClients());
        assertEquals(1, Queues.get(Queues.clientQueueName("app-12")).receive(10, 1).size());

        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-12"));
        m.startOnce();
        List<Received> acks = Queues.get(Queues.clientQueueName("app-12")).receive(10, 1);
        assertEquals(1, acks.size());
        assertEquals(Message.MessageType.TERMINATE, acks.get(0).getMessage().getType());
        assertFalse(m.isRunning());
    }

    @Test
    public void terminatingManagerRejectsNewAdmission() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-13");
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-13a"));
        m.startOnce();

        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-13a"));
        m.startOnce();
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-13b"));
        m.startOnce();

        List<Received> rejected = Queues.get(Queues.clientQueueName("app-13b")).receive(1, 1);
        assertEquals(1, rejected.size());
        assertEquals(Message.MessageType.DONE_TASK, rejected.get(0).getMessage().getType());
        assertTrue(rejected.get(0).getMessage().isError());
        assertEquals(1, m.getActiveClients(), "the draining client is untouched");
        assertTrue(m.isRunning());
    }

    @Test
    public void launchWorkersAddsReturnedIdsToTheSet() {
        Manager m = new Manager("mgr-launch") {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                assertEquals(2, count);
                return Arrays.asList("i-1", "i-2");
            }
        };

        List<String> launched = m.launchWorkers(2);

        assertEquals(Arrays.asList("i-1", "i-2"), launched);
        assertEquals(new HashSet<String>(Arrays.asList("i-1", "i-2")), m.getWorkerInstanceIds());
    }

    @Test
    public void workerLaunchScriptTerminatesItsOwnInstanceOnExit() {
        Manager m = new Manager("mgr-script");

        String script = m.workerLaunchScript("test-bucket");

        assertTrue(script.contains("aws s3 cp s3://test-bucket/" + AWS.S3_PREFIX + "Worker.jar /home/ec2-user/Worker.jar"),
                "the worker must still download its jar");
        assertTrue(script.contains("set +e\n"), "the exit block must not abort before self-termination");
        assertTrue(script.contains("STATUS=$?"), "the worker exit status must be captured");
        assertTrue(script.contains("cat /home/ec2-user/worker.log > /dev/console"),
                "the worker log must be printed before termination");
        assertTrue(script.contains("latest/meta-data/instance-id"),
                "the script must look up its own instance id");
        assertTrue(script.contains("aws ec2 terminate-instances --region us-east-1 --instance-ids \"$INSTANCE_ID\" || true"),
                "a worker exit must terminate its own instance");
        assertTrue(script.contains("exit $STATUS"), "the worker exit status must be returned");
    }

    @Test
    public void ensureWorkersCountsOnlyThisManagersWorkers() {
        final List<Integer> launchCounts = new ArrayList<Integer>();
        Manager m = new Manager("mgr-ensure", 10, 3) {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                launchCounts.add(count);
                return Collections.<String>emptyList();
            }
        };
        m.getWorkerInstanceIds().add("i-existing");

        Queues.setDev(false);
        try {
            // 6 images / 3 per worker = 2 needed; this manager already has 1 worker.
            m.ensureWorkers(6);
        } finally {
            Queues.setDev(true);
        }

        assertEquals(Arrays.asList(1), launchCounts);
    }

    @Test
    public void ensureWorkersCapsAtThisManagersInstanceLimit() {
        final List<Integer> launchCounts = new ArrayList<Integer>();
        Manager m = new Manager("mgr-cap", 10, 1) {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                launchCounts.add(count);
                return Collections.<String>emptyList();
            }
        };
        for (int i = 0; i < Manager.MAX_INSTANCES - 1; i++) {
            m.getWorkerInstanceIds().add("i-cap-" + i);
        }

        Queues.setDev(false);
        try {
            m.ensureWorkers(100);
        } finally {
            Queues.setDev(true);
        }

        assertTrue(launchCounts.isEmpty(),
                "one manager plus MAX_INSTANCES-1 workers is the per-manager cap");
    }

    @Test
    public void terminationStopsWorkersLaunchedByThisManager() {
        final List<String> terminatedIds = new ArrayList<String>();
        Manager m = new Manager("mgr-termination") {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                return Arrays.asList("i-termination-worker");
            }

            @Override
            protected void terminateInstances(List<String> instanceIds) {
                terminatedIds.addAll(instanceIds);
            }

            @Override
            protected void deleteDoneOcrQueueIfAws() {
                // Keep this offline test focused on worker termination.
            }
        };
        m.launchWorkers(1);
        Queue managerQueue = Queues.get(Queues.managerQueueName());
        Queues.register(Queues.clientQueueName("app-termination"), new InMemoryQueue());
        Queues.setDev(false);
        try {
            managerQueue.send(Message.createTerminate("app-termination"));
            m.startOnce();

            assertEquals(Arrays.asList("i-termination-worker"), terminatedIds);
            assertTrue(m.getWorkerInstanceIds().isEmpty(), "the set is cleared after termination");
            assertFalse(m.isRunning());
        } finally {
            Queues.setDev(true);
        }
    }

    @Test
    public void stopWorkersTerminatesOnlyThisManagersIds() {
        final List<String> terminatedIds = new ArrayList<String>();
        Manager mine = new Manager("mgr-mine") {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                return Arrays.asList("i-mine");
            }

            @Override
            protected void terminateInstances(List<String> instanceIds) {
                terminatedIds.addAll(instanceIds);
            }

            @Override
            protected void deleteDoneOcrQueueIfAws() {
            }
        };
        Manager other = new Manager("mgr-other") {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                return Arrays.asList("i-other");
            }
        };
        mine.launchWorkers(1);
        other.launchWorkers(1);

        Queue managerQueue = Queues.get(Queues.managerQueueName());
        Queues.register(Queues.clientQueueName("app-only-mine"), new InMemoryQueue());
        Queues.setDev(false);
        try {
            managerQueue.send(Message.createTerminate("app-only-mine"));
            mine.startOnce();

            assertEquals(Arrays.asList("i-mine"), terminatedIds);
            assertTrue(mine.getWorkerInstanceIds().isEmpty());
            assertEquals(new HashSet<String>(Arrays.asList("i-other")),
                    other.getWorkerInstanceIds(), "another manager's workers must be untouched");
        } finally {
            Queues.setDev(true);
        }
    }

    @Test
    public void devModeDoesNotAttemptWorkerTermination() {
        final List<String> terminatedIds = new ArrayList<String>();
        Manager m = new Manager("mgr-dev-termination") {
            @Override
            protected List<String> launchWorkerInstances(int count) {
                return Arrays.asList("i-dev");
            }

            @Override
            protected void terminateInstances(List<String> instanceIds) {
                terminatedIds.addAll(instanceIds);
            }
        };
        m.launchWorkers(1);

        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-dev-termination"));
        m.startOnce();

        assertTrue(terminatedIds.isEmpty(), "dev mode must not reach the AWS operation");
        assertEquals(new HashSet<String>(Arrays.asList("i-dev")), m.getWorkerInstanceIds());
    }

    @Test
    public void terminateIsIdempotent() throws Exception {
        Manager m = new Manager("mgr-14");
        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-14"));
        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-14"));
        m.startOnce();
        assertFalse(m.isRunning());
        assertEquals(1, Queues.get(Queues.clientQueueName("app-14")).receive(10, 1).size());
    }

    @Test
    public void malformedMessageIsDroppedAndLoopContinues() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-6");
        // a poison message: valid JSON, unknown type
        Message poison = Message.createDoneTask("nowhere", "app-6");
        Queues.get(Queues.managerQueueName()).send(poison);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-6"));
        m.startOnce();
        assertEquals(1, Queues.get(Queues.workerQueueName()).receive(10, 2).size());
    }

    @Test
    public void admitsUpToMaxClientsAndRequeuesTheNextOne() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-7", 2, 5);
        for (String app : new String[] {"app-7a", "app-7b", "app-7c"}) {
            Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, app));
        }
        // All three arrive in one batch: the gate was open when the batch was received.
        m.startOnce();
        assertEquals(2, m.getActiveClients());
        assertFalse(m.hasCapacity());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());

        // The full manager gives the request back to the shared queue: no client reply.
        assertTrue(Queues.get(Queues.clientQueueName("app-7c")).receive(1, 1).isEmpty(),
                "a full manager must not answer the client");
        List<Received> requeued = Queues.get(Queues.managerQueueName()).receive(10, 1);
        assertEquals(1, requeued.size());
        assertEquals(Message.MessageType.NEW_TASK, requeued.get(0).getMessage().getType());
        assertEquals("app-7c", requeued.get(0).getMessage().getLocalAppID());
        assertEquals(1, m.getRejectedClients());
        assertEquals(0, m.getServedClients());
    }

    @Test
    public void fullManagerRequeuesNewTaskAndSendsNoClientReply() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-8", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-8a"));
        m.startOnce();
        assertFalse(m.hasCapacity());

        // A request without a free slot is released back to manager_queue (for any manager to
        // admit later), not deleted and not answered with an error.
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-8b"));
        m.startOnce();
        assertTrue(Queues.get(Queues.clientQueueName("app-8b")).receive(1, 1).isEmpty(),
                "a full manager must not answer the client");
        List<Received> requeued = Queues.get(Queues.managerQueueName()).receive(10, 1);
        assertEquals(1, requeued.size());
        assertEquals("app-8b", requeued.get(0).getMessage().getLocalAppID());
        assertEquals(Message.MessageType.NEW_TASK, requeued.get(0).getMessage().getType());
        assertEquals(1, m.getRejectedClients());
        assertEquals(1, m.getActiveClients(), "the requeue must not disturb the admitted client");
    }

    @Test
    public void failedRequeueLeavesTheRequestUndeletedAndUnanswered() throws Exception {
        // A requeue that throws must not reach handle()'s catch, which deletes the message:
        // deleting it would lose the client request and the client would wait forever.
        String in = inputFile("http://a");
        ReleaseFailingQueue managerQueue = new ReleaseFailingQueue();
        Queues.register(Queues.managerQueueName(), managerQueue);
        Manager m = new Manager("mgr-14", 1, 5);

        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-14a"));
        m.startOnce(); // admits app-14a and fills the only slot
        assertFalse(m.hasCapacity());
        managerQueue.deleted.clear(); // that admission legitimately deleted its request

        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-14b"));
        m.startOnce(); // full -> release throws -> no delete

        assertTrue(managerQueue.deleted.isEmpty(), "a failed release must not delete the request");
        assertTrue(Queues.get(Queues.clientQueueName("app-14b")).receive(1, 0).isEmpty(),
                "the requeued client must not get a reply");
        assertEquals(1, m.getRejectedClients());
        assertEquals(1, m.getActiveClients());
    }

    @Test
    public void finishFreesTheSlotForTheNextClient() throws Exception {
        String in = inputFile("http://a");
        Manager m = new Manager("mgr-9", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-9a"));
        m.startOnce();
        assertFalse(m.hasCapacity());

        Queues.get(Queues.doneOcrQueueName("mgr-9"))
                .send(Message.createDoneOCR("http://a", "text", "app-9a"));
        m.startOnce();
        assertEquals(1, m.getServedClients());
        assertEquals(0, m.getActiveClients());
        assertTrue(m.hasCapacity());
        assertEquals(1, Queues.get(Queues.clientQueueName("app-9a")).receive(1, 1).size());

        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-9b"));
        m.startOnce();
        assertEquals(1, m.getActiveClients());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        assertEquals(0, m.getRejectedClients());
    }

    @Test
    public void admissionPublishesFreeSlotsImmediately() throws Exception {
        // A manager at 2/2 must advertise free=0 within one poll: admission alone publishes.
        final List<Integer> published = new ArrayList<Integer>();
        Manager m = new Manager("mgr-free", 2, 5) {
            @Override
            protected void publishFreeSlots(int free) {
                published.add(free);
            }
        };
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(inputFile("http://a"), "app-free"));
        m.startOnce();
        assertEquals(Arrays.asList(2 - 1), published, "admission must publish maxClients - 1");
    }

    @Test
    public void emptyInputFreesItsSlotAtOnce() throws Exception {
        Manager m = new Manager("mgr-10", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(inputFile(), "app-10"));
        m.startOnce();
        assertEquals(1, m.getServedClients());
        assertTrue(m.hasCapacity());
    }

    @Test
    public void duplicateNewTaskIsOneEntryAndNotRequeued() throws Exception {
        String in = inputFile("http://a", "http://b");
        Manager m = new Manager("mgr-11", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-11"));
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-11"));
        m.startOnce();
        assertEquals(1, m.getActiveClients());
        assertEquals(0, m.getRejectedClients());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        assertTrue(Queues.get(Queues.clientQueueName("app-11")).receive(1, 1).isEmpty());
        assertTrue(Queues.get(Queues.managerQueueName()).receive(10, 0).isEmpty(),
                "the duplicate is deleted, not requeued");
    }

    @Test
    public void summaryFailureAnswersErrorAndStillFreesTheSlot() throws Exception {
        String in = inputFile();
        // A directory where the summary file must be written makes writeSummary fail.
        assertTrue(new File(in + ".summary").mkdir());
        Manager m = new Manager("mgr-12", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-12"));
        m.startOnce();

        List<Received> replies = Queues.get(Queues.clientQueueName("app-12")).receive(1, 1);
        assertEquals(1, replies.size());
        assertEquals(Message.MessageType.DONE_TASK, replies.get(0).getMessage().getType());
        assertTrue(replies.get(0).getMessage().isError(), "client must be told, not left hanging");
        assertNotNull(replies.get(0).getMessage().getOutputFileLocation(),
                "summary failures need a description distinct from a busy reply");
        assertTrue(replies.get(0).getMessage().getOutputFileLocation().startsWith("Summary failure:"));
        assertEquals(0, m.getServedClients());
        assertEquals(0, m.getActiveClients());
        assertTrue(m.hasCapacity());
        assertTrue(m.isRunning());
    }

    @Test
    public void instanceIdFromConstructorIsReportedBack() {
        Manager m = new Manager("i-12345", 2, 1);
        assertEquals("i-12345", m.getManagerID());
    }

    @Test
    public void workersNeededFormula() {
        assertEquals(1, Manager.workersNeeded(1, 5));
        assertEquals(1, Manager.workersNeeded(5, 5));
        assertEquals(2, Manager.workersNeeded(6, 5));
        assertEquals(0, Manager.workersNeeded(0, 5));
    }

    @Test
    public void nonDevReadUrlsReadsFromS3() {
        // Pre-register in-memory queues so the manager (and this test) use the in-memory
        // transport even though isDev() is false: only the S3 read/write paths are simulated.
        Queues.register(Queues.managerQueueName(), new InMemoryQueue());
        Queues.register(Queues.workerQueueName(), new InMemoryQueue());
        Queues.register(Queues.doneOcrQueueName("mgr-s3"), new InMemoryQueue());
        Queues.setDev(false);
        try {
            Manager m = new Manager("mgr-s3", 10, 1) {
                @Override
                protected List<String> readUrls(String location) {
                    assertEquals("assignment1/input-files/app-x-input.txt", location);
                    return java.util.Arrays.asList("http://a", "http://b");
                }
                @Override
                protected void writeSummary(ClientTask task, String summaryKey) {
                    assertEquals("assignment1/summaries/app-1.txt", summaryKey);
                }
                @Override
                protected void ensureWorkers(int outstanding) {
                    // SQS mode must not reach EC2 from the offline suite.
                }
            };
            Queues.get(Queues.managerQueueName())
                    .send(Message.createNewTask("assignment1/input-files/app-x-input.txt", "app-1"));
            m.startOnce();
            assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size());
        } finally {
            Queues.setDev(true);
        }
    }

    /** Manager queue whose requeue always fails, recording the deletes it is asked to do. */
    private static class ReleaseFailingQueue extends InMemoryQueue {
        final List<Received> deleted = Collections.synchronizedList(new ArrayList<Received>());

        @Override
        public void delete(Received received) {
            deleted.add(received);
            super.delete(received);
        }

        @Override
        public void release(Received received) {
            throw new IllegalStateException("release failed");
        }

        @Override
        public void release(Received received, int delaySeconds) {
            throw new IllegalStateException("release failed");
        }
    }

    /** Worker queue that records every send so tests can check job ordering. */
    private static class RecordingQueue extends InMemoryQueue {
        final List<String> events;

        RecordingQueue(List<String> events) {
            this.events = events;
        }

        @Override
        public void send(Message msg) {
            events.add("send:" + msg.getImageURL());
            super.send(msg);
        }
    }

    /** Records sends and pauses the fan-out after the first batch of ten until released. */
    private static class BatchPausingQueue extends RecordingQueue {
        private final CountDownLatch firstBatchSent = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);
        private final String batchPrefix;
        private int batchSends = 0;
        private boolean paused = false;

        BatchPausingQueue(List<String> events, String batchPrefix) {
            super(events);
            this.batchPrefix = batchPrefix;
        }

        @Override
        public void send(Message msg) {
            super.send(msg);
            if (msg.getImageURL().startsWith(batchPrefix) && ++batchSends == 10 && !paused) {
                paused = true;
                firstBatchSent.countDown();
                try {
                    resume.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void awaitFirstBatch() throws InterruptedException {
            assertTrue(firstBatchSent.await(10, TimeUnit.SECONDS), "first batch was never sent");
        }

        void releaseBatch() {
            resume.countDown();
        }
    }

    @Test
    public void slowReadForOneClientDoesNotStopAnother() throws Exception {
        final CountDownLatch aReading = new CountDownLatch(1);
        final CountDownLatch releaseA = new CountDownLatch(1);
        Manager m = new Manager("mgr-slow", 2, 5) {
            @Override
            protected List<String> readUrls(String location) throws IOException {
                if ("slow-A".equals(location)) {
                    aReading.countDown();
                    try {
                        releaseA.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Arrays.asList("http://a");
            }
        };
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask("slow-A", "app-slow-a"));
        m.routerTick(); // admits A; its fan-out is queued, not run

        // A separate thread acts as the IoWorker and gets stuck in A's slow input read.
        Thread io = new Thread(new Runnable() {
            public void run() {
                m.runOnePendingJob();
            }
        });
        io.start();
        assertTrue(aReading.await(10, TimeUnit.SECONDS), "A's read should have started");

        // While A is stuck reading its input, B is admitted, OCRed and answered.
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile("http://b"), "app-slow-b"));
        m.routerTick(); // admits B
        assertTrue(m.runOnePendingJob(), "B's fan-out should run while A is stuck");
        Queues.get(Queues.doneOcrQueueName("mgr-slow"))
                .send(Message.createDoneOCR("http://b", "text-b", "app-slow-b"));
        m.routerTick(); // handles B's result and queues its finish
        assertTrue(m.runOnePendingJob(), "B's finish should run while A is stuck");
        assertEquals(1, Queues.get(Queues.clientQueueName("app-slow-b")).receive(1, 1).size());
        assertTrue(Queues.get(Queues.clientQueueName("app-slow-a")).receive(1, 1).isEmpty());
        assertEquals(1, m.getActiveClients(), "A must still be in flight");

        releaseA.countDown();
        io.join(10000);
        assertFalse(io.isAlive());
        assertEquals(2, Queues.get(Queues.workerQueueName()).receive(10, 1).size(),
                "A's image task is sent once its read completes");
    }

    @Test
    public void twoClientsInterleaveAndBothAreAnswered() throws Exception {
        Manager m = new Manager("mgr-inter", 2, 5);
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile("http://a1", "http://a2"), "app-inter-a"));
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile("http://b1", "http://b2"), "app-inter-b"));
        m.startOnce(); // both fan-outs run
        assertEquals(4, Queues.get(Queues.workerQueueName()).receive(10, 1).size());

        Queue doneOcr = Queues.get(Queues.doneOcrQueueName("mgr-inter"));
        doneOcr.send(Message.createDoneOCR("http://a1", "a1", "app-inter-a"));
        m.startOnce();
        doneOcr.send(Message.createDoneOCR("http://b1", "b1", "app-inter-b"));
        m.startOnce();
        doneOcr.send(Message.createDoneOCR("http://a2", "a2", "app-inter-a"));
        doneOcr.send(Message.createDoneOCR("http://b2", "b2", "app-inter-b"));
        m.startOnce(); // both clients complete

        assertEquals(1, Queues.get(Queues.clientQueueName("app-inter-a")).receive(10, 1).size());
        assertEquals(1, Queues.get(Queues.clientQueueName("app-inter-b")).receive(10, 1).size());
        assertEquals(2, m.getServedClients());
        assertEquals(0, m.getActiveClients());
    }

    @Test
    public void duplicateDoneOcrYieldsExactlyOneSummary() throws Exception {
        String in = inputFile("http://x");
        Manager m = new Manager("mgr-dup", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-dup"));
        m.startOnce();
        Queue doneOcr = Queues.get(Queues.doneOcrQueueName("mgr-dup"));
        doneOcr.send(Message.createDoneOCR("http://x", "first", "app-dup"));
        doneOcr.send(Message.createDoneOCR("http://x", "second", "app-dup")); // duplicate result
        m.startOnce();

        assertEquals(1, Queues.get(Queues.clientQueueName("app-dup")).receive(10, 1).size(),
                "the client is answered exactly once");
        assertEquals(1, m.getServedClients());
        Scanner scanner = new Scanner(new File(in + ".summary"));
        String summary = scanner.useDelimiter("\\A").next();
        scanner.close();
        assertTrue(summary.contains("first"), "putIfAbsent keeps the first text");
        assertFalse(summary.contains("second"), "a duplicate DONE_OCR must not overwrite");
    }

    @Test
    public void finishRunsBeforeAQueuedFanOut() throws Exception {
        List<String> events = new ArrayList<String>();
        Queues.register(Queues.workerQueueName(), new RecordingQueue(events));
        String inA = inputFile("http://a1");
        String inB = inputFile("http://b1");
        Manager m = new Manager("mgr-fo", 2, 5) {
            @Override
            protected void writeSummary(ClientTask task, String summaryLocation) throws IOException {
                events.add("finish:" + task.localAppID);
                super.writeSummary(task, summaryLocation);
            }
        };
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(inA, "app-fo-a"));
        m.startOnce(); // A admitted and its fan-out runs: one image task sent
        assertEquals(1, events.size());

        // Queue B's fan-out first, then A's finish behind it in the job queue.
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(inB, "app-fo-b"));
        m.routerTick(); // admits B; its fan-out stays queued
        Queues.get(Queues.doneOcrQueueName("mgr-fo"))
                .send(Message.createDoneOCR("http://a1", "text-a", "app-fo-a"));
        m.routerTick(); // records A's result; its finish is queued behind B's fan-out

        m.runPendingIo(); // the finish must outrank the queued fan-out
        assertEquals(1, events.indexOf("finish:app-fo-a"), "finish ran before B's fan-out");
        assertEquals(2, events.indexOf("send:http://b1"));
        assertEquals(1, Queues.get(Queues.clientQueueName("app-fo-a")).receive(1, 1).size());
    }

    @Test
    public void finishRunsBetweenFanOutBatches() throws Exception {
        List<String> events = new ArrayList<String>();
        final BatchPausingQueue workerQueue = new BatchPausingQueue(events, "http://d");
        Queues.register(Queues.workerQueueName(), workerQueue);
        Manager m = new Manager("mgr-batch", 2, 5) {
            @Override
            protected void writeSummary(ClientTask task, String summaryLocation) throws IOException {
                events.add("finish:" + task.localAppID);
                super.writeSummary(task, summaryLocation);
            }
        };

        // Client C: one image, its fan-out completes at once.
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile("http://c1"), "app-batch-c"));
        m.startOnce();
        assertEquals(1, events.size());

        // Client D: 25 URLs, i.e. three batches of 10/10/5. Its fan-out is queued.
        String[] manyUrls = new String[25];
        for (int i = 0; i < manyUrls.length; i++) {
            manyUrls[i] = String.format("http://d%02d", i);
        }
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile(manyUrls), "app-batch-d"));
        m.routerTick(); // admits D; its fan-out stays queued

        // A thread acts as the IoWorker and runs D's fan-out; it pauses after batch one.
        Thread io = new Thread(new Runnable() {
            public void run() {
                m.runOnePendingJob();
            }
        });
        io.start();
        workerQueue.awaitFirstBatch();

        // The router answers C while D's fan-out is parked between batches.
        Queues.get(Queues.doneOcrQueueName("mgr-batch"))
                .send(Message.createDoneOCR("http://c1", "text-c", "app-batch-c"));
        m.routerTick(); // queues C's finish behind the running fan-out
        workerQueue.releaseBatch();
        io.join(10000);
        assertFalse(io.isAlive());

        // events: 1 send for C, 10 sends of batch one, then C's finish, then batch two and three.
        assertEquals(11, events.indexOf("finish:app-batch-c"), "finish ran between the batches");
        assertEquals(27, events.size(), "all 25 image tasks plus C's were sent");
        assertEquals(1, Queues.get(Queues.clientQueueName("app-batch-c")).receive(1, 1).size());
    }

    @Test
    public void summaryPreservesInputUrlOrder() throws Exception {
        String in = inputFile("zeta", "alpha", "mike");
        Manager m = new Manager("mgr-order", 1, 5);
        Queues.get(Queues.managerQueueName()).send(Message.createNewTask(in, "app-order"));
        m.startOnce();
        Queue doneOcr = Queues.get(Queues.doneOcrQueueName("mgr-order"));
        // Results arrive in an order unrelated to the input order.
        doneOcr.send(Message.createDoneOCR("mike", "3", "app-order"));
        m.startOnce();
        doneOcr.send(Message.createDoneOCR("alpha", "2", "app-order"));
        m.startOnce();
        doneOcr.send(Message.createDoneOCR("zeta", "1", "app-order"));
        m.startOnce();

        assertEquals(Arrays.asList("zeta\t1", "alpha\t2", "mike\t3"),
                Files.readAllLines(Paths.get(in + ".summary")),
                "summary lines follow input URL order, not map order");
    }

    @Test
    public void reconcileRemovesTerminatedWorker() {
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-dead", "terminated");
        Manager m = reconcileManager(states, new ArrayList<Integer>());
        m.getWorkerInstanceIds().add("i-dead");

        reconcile(m);

        assertTrue(m.getWorkerInstanceIds().isEmpty(), "a terminated worker must be removed");
    }

    @Test
    public void reconcileRemovesStoppedWorker() {
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-dead", "stopped");
        Manager m = reconcileManager(states, new ArrayList<Integer>());
        m.getWorkerInstanceIds().add("i-dead");

        reconcile(m);

        assertTrue(m.getWorkerInstanceIds().isEmpty(), "a stopped worker must be removed");
    }

    @Test
    public void reconcileRemovesMissingWorker() {
        Map<String, String> states = new HashMap<String, String>();
        Manager m = reconcileManager(states, new ArrayList<Integer>());
        m.getWorkerInstanceIds().add("i-gone");

        reconcile(m);

        assertTrue(m.getWorkerInstanceIds().isEmpty(), "a worker absent from EC2 must be removed");
    }

    @Test
    public void reconcileKeepsRunningWorker() {
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-live", "running");
        Manager m = reconcileManager(states, new ArrayList<Integer>());
        m.getWorkerInstanceIds().add("i-live");

        reconcile(m);

        assertEquals(new HashSet<String>(Arrays.asList("i-live")), m.getWorkerInstanceIds(),
                "a running worker must stay in the set");
    }

    @Test
    public void reconcileLaunchesReplacementWhenDeadWorkerRemovedAndWorkOutstanding() throws Exception {
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-dead", "terminated");
        final List<Integer> launchCounts = new ArrayList<Integer>();
        Manager m = new Manager("mgr-reconcile-replace", 10, 1) {
            @Override
            protected Map<String, String> workerStates(Collection<String> instanceIds) {
                return states;
            }

            @Override
            protected List<String> launchWorkerInstances(int count) {
                launchCounts.add(count);
                List<String> ids = new ArrayList<String>();
                for (int i = 0; i < count; i++) {
                    ids.add("i-new-" + i);
                }
                return ids;
            }
        };
        m.getWorkerInstanceIds().add("i-dead");
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask(inputFile("http://a", "http://b"), "app-reconcile"));
        m.startOnce(); // dev mode: admits the client and leaves two images outstanding

        Queues.setDev(false);
        try {
            m.reconcileWorkers();
        } finally {
            Queues.setDev(true);
        }

        assertFalse(m.getWorkerInstanceIds().contains("i-dead"));
        assertEquals(Arrays.asList(2), launchCounts, "two outstanding images need two workers");
        assertTrue(m.getWorkerInstanceIds().containsAll(Arrays.asList("i-new-0", "i-new-1")));
    }

    @Test
    public void reconcileDoesNotLaunchWhenNoWorkOutstanding() {
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-dead", "terminated");
        final List<Integer> launchCounts = new ArrayList<Integer>();
        Manager m = reconcileManager(states, launchCounts);
        m.getWorkerInstanceIds().add("i-dead");

        Queues.setDev(false);
        try {
            m.reconcileWorkers();
        } finally {
            Queues.setDev(true);
        }

        assertTrue(m.getWorkerInstanceIds().isEmpty());
        assertTrue(launchCounts.isEmpty(), "no outstanding work means no replacement");
    }

    @Test
    public void reconcileSkipsWhileTerminating() {
        final List<Integer> stateCalls = new ArrayList<Integer>();
        Manager m = new Manager("mgr-reconcile-terminating") {
            @Override
            protected Map<String, String> workerStates(Collection<String> instanceIds) {
                stateCalls.add(instanceIds.size());
                return Collections.<String, String>emptyMap();
            }

            @Override
            protected void terminateInstances(List<String> instanceIds) {
            }

            @Override
            protected void deleteDoneOcrQueueIfAws() {
            }
        };
        m.getWorkerInstanceIds().add("i-still-there");
        Queues.get(Queues.managerQueueName()).send(Message.createTerminate("app-reconcile"));
        m.startOnce();
        assertFalse(m.isRunning());

        m.reconcileWorkers();

        assertTrue(stateCalls.isEmpty(), "reconcile must not run while terminating");
        assertEquals(new HashSet<String>(Arrays.asList("i-still-there")), m.getWorkerInstanceIds());
    }

    /** Runs reconcile with AWS mode on; the test manager overrides every AWS seam. */
    private void reconcile(Manager m) {
        Queues.setDev(false);
        try {
            m.reconcileWorkers();
        } finally {
            Queues.setDev(true);
        }
    }

    private Manager reconcileManager(final Map<String, String> states, final List<Integer> launchCounts) {
        return new Manager("mgr-reconcile", 10, 1) {
            @Override
            protected Map<String, String> workerStates(Collection<String> instanceIds) {
                return states;
            }

            @Override
            protected List<String> launchWorkerInstances(int count) {
                launchCounts.add(count);
                return Collections.<String>emptyList();
            }
        };
    }

    @Test
    public void failedNewTaskRepliesWithErrorInsteadOfSilentlyDropping() throws Exception {
        Manager m = new Manager("mgr-err", 10, 1) {
            @Override
            protected List<String> readUrls(String location) throws IOException {
                throw new IOException("S3 unavailable");
            }
        };
        m.startOnce();
        Queues.get(Queues.managerQueueName())
                .send(Message.createNewTask("in.txt", "app-err"));
        m.startOnce();
        List<Received> replies = Queues.get(Queues.clientQueueName("app-err")).receive(1, 1);
        assertEquals(1, replies.size());
        assertTrue(replies.get(0).getMessage().isError());
    }
}
