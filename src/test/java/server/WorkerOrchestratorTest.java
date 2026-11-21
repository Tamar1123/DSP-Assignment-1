package server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import utils.Queues;

// Worker sizing: each manager sizes its own share of the fleet from the images it owns.
class WorkerOrchestratorTest {

    // Simulated EC2 fleet shared by every orchestrator under test.
    private static final class FakeFleet {
        private final int managerCount;
        private final List<String> workers = new ArrayList<String>();
        private int nextId;

        FakeFleet(int managerCount) {
            this.managerCount = managerCount;
        }

        int workerCount() {
            return workers.size();
        }

        int instanceCount() {
            return workers.size() + managerCount;
        }

        List<String> launch(String prefix, int count) {
            List<String> ids = new ArrayList<String>();
            for (int i = 0; i < count; i++) {
                String id = prefix + "-worker-" + nextId++;
                workers.add(id);
                ids.add(id);
            }
            return ids;
        }
    }

    private static class FakePort implements WorkerOrchestrator.Port {
        private final FakeFleet fleet;
        private final String prefix;
        private final List<Integer> launchCounts = new ArrayList<Integer>();

        FakePort(FakeFleet fleet, String prefix) {
            this.fleet = fleet;
            this.prefix = prefix;
        }

        public int liveInstanceCount() { return fleet.instanceCount(); }

        public List<String> launchWorkerInstances(int count) {
            launchCounts.add(count);
            return fleet.launch(prefix, count);
        }

        public void terminateInstances(List<String> ids) { }

        public Map<String, String> workerStates(Collection<String> ids) {
            return new HashMap<String, String>();
        }

        public void ensureWorkers(int outstanding) { }

        public int totalOutstanding() { return 0; }

        public boolean isTerminating() { return false; }

        public void log(String message) { }
    }

    private static void ensureInAwsMode(WorkerOrchestrator... orchestrators) {
        Queues.setDev(false);
        try {
            for (WorkerOrchestrator orchestrator : orchestrators) {
                orchestrator.ensure(12);
            }
        } finally {
            Queues.setDev(true);
        }
    }

    @Test
    public void twoManagersEachSizeTheirOwnShare() {
        FakeFleet fleet = new FakeFleet(2);
        FakePort aPort = new FakePort(fleet, "a");
        FakePort bPort = new FakePort(fleet, "b");
        WorkerOrchestrator a = new WorkerOrchestrator(aPort, 4);
        WorkerOrchestrator b = new WorkerOrchestrator(bPort, 4);

        Queues.setDev(false);
        try {
            a.ensure(12); // manager A owns 12 images: 3 workers of its own
            b.ensure(12); // manager B owns 12 more: 3 workers of its own, not "fleet covered"
        } finally {
            Queues.setDev(true);
        }

        assertEquals(Collections.singletonList(3), aPort.launchCounts);
        assertEquals(Collections.singletonList(3), bPort.launchCounts);
        assertEquals(6, fleet.workerCount(), "24 images at 4 per worker need 6 workers");
        assertEquals(6, a.ids().size() + b.ids().size());
    }

    @Test
    public void noLaunchWhenOwnWorkersCoverTheNeed() {
        FakeFleet fleet = new FakeFleet(1);
        FakePort port = new FakePort(fleet, "a");
        WorkerOrchestrator orchestrator = new WorkerOrchestrator(port, 4);

        ensureInAwsMode(orchestrator); // first call launches this manager's 3 workers
        ensureInAwsMode(orchestrator); // same 12 images: its own 3 workers already cover them

        assertEquals(Collections.singletonList(3), port.launchCounts);
        assertEquals(3, fleet.workerCount());
    }

    @Test
    public void headroomClampsTheLaunch() {
        FakeFleet fleet = new FakeFleet(8); // eight instances live, one below MAX_INSTANCES
        FakePort port = new FakePort(fleet, "a");
        WorkerOrchestrator orchestrator = new WorkerOrchestrator(port, 4);

        ensureInAwsMode(orchestrator);

        assertEquals(Collections.singletonList(1), port.launchCounts,
                "the global cap leaves room for one more instance");
    }

    @Test
    public void noLaunchAtTheGlobalCap() {
        FakeFleet fleet = new FakeFleet(WorkerOrchestrator.MAX_INSTANCES);
        FakePort port = new FakePort(fleet, "a");
        WorkerOrchestrator orchestrator = new WorkerOrchestrator(port, 4);

        ensureInAwsMode(orchestrator);

        assertTrue(port.launchCounts.isEmpty(), "a fleet at MAX_INSTANCES must not launch");
    }

    @Test
    public void concurrentEnsureCallsLaunchOnlyOnce() throws Exception {
        final FakeFleet fleet = new FakeFleet(1);
        FakePort port = new FakePort(fleet, "a") {
            public List<String> launchWorkerInstances(int count) {
                try {
                    Thread.sleep(200); // a slow EC2 call widens the race window
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                synchronized (fleet) {
                    return super.launchWorkerInstances(count);
                }
            }
        };
        final WorkerOrchestrator orchestrator = new WorkerOrchestrator(port, 4);

        Queues.setDev(false);
        try {
            Runnable call = new Runnable() {
                public void run() { orchestrator.ensure(12); }
            };
            Thread t1 = new Thread(call);
            Thread t2 = new Thread(call);
            t1.start();
            t2.start();
            t1.join();
            t2.join();
        } finally {
            Queues.setDev(true);
        }

        assertEquals(3, fleet.workerCount(), "two racing ensure calls must not double-provision");
    }

    @Test
    public void workersToLaunchBoundaries() {
        assertEquals(0, WorkerOrchestrator.workersToLaunch(0, 4, 0, 2));
        assertEquals(0, WorkerOrchestrator.workersToLaunch(-1, 4, 0, 2));
        assertEquals(0, WorkerOrchestrator.workersToLaunch(12, 4, 3, 2),
                "own workers already cover the need");
        assertEquals(0, WorkerOrchestrator.workersToLaunch(12, 4, 5, 2),
                "more own workers than needed must not go negative");
        assertEquals(3, WorkerOrchestrator.workersToLaunch(12, 4, 0, 2));
        assertEquals(4, WorkerOrchestrator.workersToLaunch(13, 4, 0, 2));
        assertEquals(1, WorkerOrchestrator.workersToLaunch(100, 4, 0, 8),
                "headroom clamps a large need");
    }
}
