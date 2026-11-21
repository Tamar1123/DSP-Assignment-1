package utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stops only one manager's workers. These tests use the pure {@link WorkerTermination}
 * seam with a fake EC2 and a fake sleeper: no AWS client is constructed and no real
 * sleeping happens. There is no timeout; the wait lasts until the fake reports the
 * instances settled.
 */
public class WorkerTerminationTest {

    @Test
    public void terminateWorkersOfFiltersByManagerID() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-running", "running", "worker", "mgr-A"));
        ec2.add(instance("i-a-pending", "pending", "worker", "mgr-A"));
        ec2.add(instance("i-b-running", "running", "worker", "mgr-B"));
        // Already stopped: not a live worker, so it must not be targeted.
        ec2.add(instance("i-a-stopped", "stopped", "worker", "mgr-A"));

        FakeSleeper sleeper = new FakeSleeper();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 100L, sleeper);

        assertEquals(Arrays.asList("i-a-running", "i-a-pending"), ec2.terminated);
        assertEquals(Arrays.asList("i-a-running", "i-a-pending"), result.targeted);
        assertFalse(result.targeted.contains("i-b-running"));
        assertEquals(0, sleeper.sleeps, "already-terminated workers settle on the first probe");
    }

    @Test
    public void terminateWorkersOfWithNoMatchingInstancesReturnsWithoutError() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-b-running", "running", "worker", "mgr-B"));

        FakeSleeper sleeper = new FakeSleeper();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 100L, sleeper);

        assertTrue(result.targeted.isEmpty());
        assertTrue(ec2.terminated.isEmpty());
        assertEquals(0, ec2.terminateCalls, "terminate must never be called when nothing matched");
        assertEquals(0, ec2.probeCalls, "nothing to poll when no instance matches");
        assertEquals(0, sleeper.sleeps);
    }

    @Test
    public void stopWorkersKeepsPollingUntilSettled() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-slow", "running", "worker", "mgr-A"));
        ec2.settleAfterProbes = 3;

        FakeSleeper sleeper = new FakeSleeper();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 100L, sleeper);

        assertEquals(Arrays.asList("i-a-slow"), result.targeted);
        assertEquals(Arrays.asList("i-a-slow"), ec2.terminated);
        assertEquals(3, ec2.probeCalls, "the worker settles on the third probe");
        assertEquals(2, sleeper.sleeps, "it waits between probes until the worker settles");
    }

    @Test
    public void stopWorkersKeepsWaitingWhenStatesAreMissing() {
        FakeEc2 ec2 = new FakeEc2();
        ec2.add(instance("i-a-flaky", "running", "worker", "mgr-A"));
        ec2.settleAfterProbes = 3;
        // The first two probes report nothing: a transient/incomplete DescribeInstances response.
        ec2.omitStatesForProbes = 2;

        FakeSleeper sleeper = new FakeSleeper();
        WorkerTermination.StopResult result =
                WorkerTermination.stopWorkers("mgr-A", ec2, 100L, sleeper);

        assertEquals(Arrays.asList("i-a-flaky"), result.targeted);
        assertEquals(3, ec2.probeCalls,
                "a missing state must read as 'still stopping', not as 'already gone'");
        assertEquals(2, sleeper.sleeps);
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
        /** 0 means terminate settles immediately; N > 0 means settle on probe N. */
        int settleAfterProbes;
        /** Number of initial probes that report no state at all. */
        int omitStatesForProbes;
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
            if (settleAfterProbes <= 0) {
                settled.addAll(instanceIds);
            }
        }

        @Override
        public Map<String, String> states(List<String> instanceIds) {
            probeCalls++;
            Map<String, String> states = new HashMap<String, String>();
            if (probeCalls <= omitStatesForProbes) {
                return states;
            }
            if (settleAfterProbes > 0 && probeCalls >= settleAfterProbes) {
                settled.addAll(instanceIds);
            }
            for (String id : instanceIds) {
                states.put(id, settled.contains(id) ? "terminated" : "running");
            }
            return states;
        }
    }

    /** A fake sleeper: counts sleeps without blocking. */
    private static final class FakeSleeper implements WorkerTermination.Sleeper {
        int sleeps;

        @Override
        public void sleep(long millis) {
            sleeps++;
        }
    }
}
