package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure decision and polling logic for stopping the workers one manager launched.
 *
 * <p>The EC2 calls live behind {@link Ec2Ops}, which {@link AWS} implements with its
 * client. This class uses no AWS SDK types, so the selection predicate and the wait
 * can be tested offline with a fake EC2 and a fake sleeper.</p>
 */
public final class WorkerTermination {

    /** EC2 {@code Name} tag value identifying a worker instance. */
    public static final String WORKER_TAG = "worker";
    /** EC2 tag key carrying the ID of the manager that launched the instance. */
    public static final String MANAGER_TAG = "managerID";
    /** EC2 tag key usually carrying the instance role name. */
    public static final String NAME_TAG = "Name";

    /** Default delay (milliseconds) between state probes while workers settle. */
    public static final long POLL_INTERVAL_MILLIS = 2_000L;

    private WorkerTermination() {
    }

    /** One instance as seen by {@code DescribeInstances}, without any AWS SDK type. */
    public static final class InstanceRef {
        public final String id;
        public final String state;
        public final Map<String, String> tags;

        public InstanceRef(String id, String state, Map<String, String> tags) {
            this.id = id;
            this.state = state;
            this.tags = tags;
        }

        /** Value of one tag, or null when absent. */
        public String tag(String key) {
            return tags == null ? null : tags.get(key);
        }
    }

    /** The EC2 operations the stop flow needs; {@link AWS} adapts its client to this. */
    public interface Ec2Ops {
        /** Instances that may belong to {@code managerID}, for filtering here. */
        List<InstanceRef> describeManagedInstances(String managerID);

        /** Requests termination of the given instance IDs. */
        void terminate(List<String> instanceIds);

        /**
         * Current state per instance ID. An ID missing from the map (or mapped to null)
         * means its state is not yet confirmed stopped, so the wait keeps polling.
         */
        Map<String, String> states(List<String> instanceIds);
    }

    /** Blocks until the clock reaches the deadline; injectable so tests never sleep. */
    public interface Sleeper {
        void sleep(long millis);
    }

    /** Outcome of one {@link #stopWorkers} call. */
    public static final class StopResult {
        /** Worker IDs selected for termination (empty when nothing matched). */
        public final List<String> targeted;

        StopResult(List<String> targeted) {
            this.targeted = Collections.unmodifiableList(new ArrayList<String>(targeted));
        }

        public boolean foundAny() {
            return !targeted.isEmpty();
        }
    }

    /**
     * Terminates the running/pending workers tagged {@code Name=worker} and
     * {@code managerID=managerID}, then polls until they are terminated or shutting down.
     * There is no timeout: per the assignment the manager waits for its workers to finish.
     * Never sleeps for real beyond the injected sleeper.
     */
    public static StopResult stopWorkers(String managerID, Ec2Ops ec2, long pollMillis,
                                         Sleeper sleeper) {
        if (ec2 == null || isBlank(managerID)) {
            return emptyResult();
        }
        List<String> ids = selectWorkerIds(ec2.describeManagedInstances(managerID), managerID);
        if (ids.isEmpty()) {
            return emptyResult();
        }

        ec2.terminate(ids);

        long interval = pollMillis > 0L ? pollMillis : POLL_INTERVAL_MILLIS;
        while (true) {
            if (unsettled(ids, ec2.states(ids)).isEmpty()) {
                return new StopResult(ids);
            }
            sleeper.sleep(interval);
        }
    }

    private static StopResult emptyResult() {
        return new StopResult(Collections.<String>emptyList());
    }

    /** IDs of instances that are live workers launched by {@code managerID}. */
    public static List<String> selectWorkerIds(List<InstanceRef> instances, String managerID) {
        if (instances == null || isBlank(managerID)) {
            return Collections.emptyList();
        }
        Set<String> ids = new LinkedHashSet<String>();
        for (InstanceRef instance : instances) {
            if (isWorkerOf(instance, managerID)) {
                ids.add(instance.id);
            }
        }
        return new ArrayList<String>(ids);
    }

    /** True for a running/pending instance tagged {@code Name=worker} for this manager. */
    public static boolean isWorkerOf(InstanceRef instance, String managerID) {
        return instance != null
                && instance.id != null
                && !isBlank(managerID)
                && WORKER_TAG.equals(instance.tag(NAME_TAG))
                && managerID.equals(instance.tag(MANAGER_TAG))
                && isLiveState(instance.state);
    }

    /** Running or pending, mirroring the existing live-instance count. */
    public static boolean isLiveState(String state) {
        return "running".equals(state) || "pending".equals(state);
    }

    /** A worker that reached terminated or shutting-down no longer needs waiting for. */
    public static boolean isSettledState(String state) {
        return "terminated".equals(state) || "shutting-down".equals(state);
    }

    /**
     * IDs from {@code ids} that are not confirmed settled. Only {@code terminated} and
     * {@code shutting-down} count as settled; a missing or null state is unsettled, since a
     * transient or incomplete {@code DescribeInstances} response must not be read as
     * "already gone". During the wait this keeps the caller polling.
     */
    public static List<String> unsettled(List<String> ids, Map<String, String> states) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> still = new ArrayList<String>();
        for (String id : ids) {
            String state = states == null ? null : states.get(id);
            if (!isSettledState(state)) {
                still.add(id);
            }
        }
        return still;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
