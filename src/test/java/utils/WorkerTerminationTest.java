package utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stops only one manager's workers. These tests use the pure {@link WorkerTermination}
 * seam with a fake EC2 and a fake clock: no AWS client is constructed and no real
 * sleeping happens.
 */
public class WorkerTerminationTest {

    @Test
    public void terminateWorkersOfFiltersByManagerID() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-running", "running", "worker", "mgr-A"));
        ec2.add(instance("i-a-pending", "pending", "worker", "mgr-A"));
        ec2.add(instance("i-b-running", "running", "worker", "mgr-B"));
        // Same manager, but not a worker: the manager must not terminate itself here.
        ec2.add(instance("i-a-manager", "running", "manager", "mgr-A"));
        // Already stopped: not a running/pending worker, so not targeted.
        ec2.add(instance("i-a-stopped", "stopped", "worker", "mgr-A"));

        FakeClock clock = new FakeClock();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 1000L, 100L, clock, clock);

        assertEquals(Arrays.asList("i-a-running", "i-a-pending"), ec2.terminated);
        assertEquals(Arrays.asList("i-a-running", "i-a-pending"), result.targeted);
        assertFalse(result.timedOut);
        assertTrue(result.survivors.isEmpty());
        assertFalse(result.targeted.contains("i-b-running"));
        assertEquals(0, clock.sleeps, "already-terminated workers settle on the first probe");
    }

    @Test
    public void terminateWorkersOfWithNoMatchingInstancesReturnsWithoutError() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-b-running", "running", "worker", "mgr-B"));

        FakeClock clock = new FakeClock();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 1000L, 10L, clock, clock);

        assertTrue(result.targeted.isEmpty());
        assertTrue(result.survivors.isEmpty());
        assertFalse(result.timedOut);
        assertTrue(ec2.terminated.isEmpty());
        assertEquals(0, ec2.terminateCalls, "terminate must never be called when nothing matched");
        assertEquals(0, ec2.probeCalls, "nothing to poll when no instance matches");
        assertEquals(0, clock.sleeps);
    }

    @Test
    public void terminateWorkersOfIsBoundedOnTimeout() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-stuck", "running", "worker", "mgr-A"));
        // This instance never reaches terminated/shutting-down.
        ec2.neverSettles = true;

        FakeClock clock = new FakeClock();
        long timeoutMillis = 120L;
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, timeoutMillis, 25L, clock, clock);

        assertTrue(result.timedOut);
        assertEquals(Arrays.asList("i-a-stuck"), ec2.terminated);
        assertEquals(Arrays.asList("i-a-stuck"), result.survivors);
        assertEquals(timeoutMillis, clock.now - FakeClock.START,
                "the wait must stop at the deadline, not overrun it");
        assertTrue(clock.sleeps > 0, "the fake clock advanced instead of sleeping for real");
        assertEquals(clock.sleeps, ec2.probeCalls,
                "every sleep must be followed by a probe, but no probe may run once the "
                        + "deadline has passed (the last sleep lands on the deadline, and the "
                        + "loop must exit before probing again)");
    }

    @Test
    public void zeroTimeoutProbesOnceAndReturnsTimedOutWithoutSleeping() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-stuck", "running", "worker", "mgr-A"));
        ec2.neverSettles = true;

        FakeClock clock = new FakeClock();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 0L, 25L, clock, clock);

        assertTrue(result.timedOut);
        assertEquals(Arrays.asList("i-a-stuck"), result.survivors);
        assertEquals(1, ec2.probeCalls,
                "the very first probe runs even at the deadline, but no probe follows it");
        assertEquals(0, clock.sleeps, "no sleep once the deadline has already passed");
    }

    @Test
    public void stopWorkersKeepsWaitingWhenStatesAreMissing() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-flaky", "running", "worker", "mgr-A"));
        ec2.neverSettles = true;
        // The probe reports nothing: a transient/incomplete DescribeInstances response.
        ec2.omitStates = true;

        FakeClock clock = new FakeClock();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 100L, 25L, clock, clock);

        assertTrue(result.timedOut,
                "a missing state must read as 'still stopping', not as 'already gone'");
        assertEquals(Arrays.asList("i-a-flaky"), result.survivors);
    }

    @Test
    public void unsettledTreatsMissingOrNullStateAsUnsettled() {
        List<String> ids = Arrays.asList("i-terminated", "i-null-state", "i-missing");
        Map<String, String> states = new HashMap<String, String>();
        states.put("i-terminated", "terminated");
        states.put("i-null-state", null);
        // "i-missing" has no entry at all.

        List<String> still = WorkerTermination.unsettled(ids, states);

        assertEquals(Arrays.asList("i-null-state", "i-missing"), still,
                "only terminated/shutting-down count as settled; a null or missing state "
                        + "must keep the instance on the waiting list");
    }

    @Test
    public void unsettledTreatsANullStatesMapAsAllUnsettled() {
        List<String> ids = Arrays.asList("i-1", "i-2");

        assertEquals(ids, WorkerTermination.unsettled(ids, null));
    }

    private static WorkerTermination.InstanceRef instance(
            String id, String state, String nameTag, String managerTag) {
        Map<String, String> tags = new HashMap<>();
        tags.put(WorkerTermination.NAME_TAG, nameTag);
        tags.put(WorkerTermination.MANAGER_TAG, managerTag);
        return new WorkerTermination.InstanceRef(id, state, tags);
    }

    /** Records terminate calls and reports a state per instance id. */
    private static final class FakeEc2 implements WorkerTermination.Ec2Ops {
        final List<WorkerTermination.InstanceRef> instances =
                new ArrayList<WorkerTermination.InstanceRef>();
        final List<String> terminated = new ArrayList<String>();
        final Set<String> settled = new HashSet<String>();
        boolean neverSettles;
        boolean omitStates;
        int terminateCalls;
        int probeCalls;

        void add(WorkerTermination.InstanceRef instance) {
            instances.add(instance);
        }

        @Override
        public List<WorkerTermination.InstanceRef> describeManagedInstances(String managerID) {
            return new ArrayList<WorkerTermination.InstanceRef>(instances);
        }

        @Override
        public void terminate(List<String> instanceIds) {
            terminateCalls++;
            terminated.addAll(instanceIds);
            if (!neverSettles) {
                settled.addAll(instanceIds);
            }
        }

        @Override
        public Map<String, String> states(List<String> instanceIds) {
            probeCalls++;
            Map<String, String> states = new HashMap<String, String>();
            if (omitStates) {
                return states;
            }
            for (String id : instanceIds) {
                states.put(id, settled.contains(id) ? "terminated" : "running");
            }
            return states;
        }
    }

    /** A fake clock that also acts as the sleeper: sleeping advances time instantly. */
    private static final class FakeClock implements LongSupplier, WorkerTermination.Sleeper {
        static final long START = 1000L;
        long now = START;
        int sleeps;

        @Override
        public long getAsLong() {
            return now;
        }

        @Override
        public void sleep(long millis) {
            sleeps++;
            now += millis;
        }
    }
}
