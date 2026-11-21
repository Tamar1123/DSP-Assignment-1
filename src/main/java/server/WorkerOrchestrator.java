package server;

import utils.AWS;
import utils.Queues;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Owns the worker instances one manager launched: sizing, launch, crash replacement, shutdown.
// Calls back through Port for everything that touches AWS, so tests can stub it.
class WorkerOrchestrator {
    interface Port {
        int liveWorkerCount();
        int liveInstanceCount();
        List<String> launchWorkerInstances(int count);
        void terminateInstances(List<String> ids);
        Map<String, String> workerStates(Collection<String> ids);
        void ensureWorkers(int outstanding);
        int totalOutstanding();
        boolean isTerminating();
        void log(String message);
    }

    // Max EC2 instances (managers plus workers) the system may run.
    static final int MAX_INSTANCES = 9;
    static final String WORKER_TAG = "worker";
    private static final String WORKER_JAR_KEY = AWS.S3_PREFIX + "Worker.jar";
    private static final String WORKER_JAR_PATH = "/home/ec2-user/Worker.jar";
    private static final String DEBUG_LOG_UPLOAD =
            "touch /home/ec2-user/worker.log\n(while true; do aws s3 cp /home/ec2-user/worker.log s3://"
            + "@BUCKET@/" + AWS.S3_PREFIX + "logs/worker-$(hostname).log --quiet; sleep 5; done) &\n";

    private final Port port;
    private final int workerFilesN;
    // Worker instance IDs this manager launched; the source of truth for its workers.
    private final Set<String> workerInstanceIds = ConcurrentHashMap.newKeySet();

    WorkerOrchestrator(Port port, int workerFilesN) {
        this.port = port;
        this.workerFilesN = workerFilesN;
    }

    // ceil(urlCount / workerFilesN); zero with no URLs.
    static int workersNeeded(int urlCount, int workerFilesN) {
        if (workerFilesN < 1) {
            throw new IllegalArgumentException("worker-files-n must be at least 1, got " + workerFilesN);
        }
        if (urlCount <= 0) {
            return 0;
        }
        return (urlCount - 1) / workerFilesN + 1;
    }

    Set<String> ids() {
        return workerInstanceIds;
    }

    // Starts the workers needed for the outstanding images, sized against the whole fleet and
    // capped by the global instance limit.
    void ensure(int outstanding) {
        if (Queues.isDev() || outstanding <= 0) {
            return;
        }
        try {
            int needed = workersNeeded(outstanding, workerFilesN);
            int liveWorkers = port.liveWorkerCount();
            int liveInstances = port.liveInstanceCount();
            int toStart = Math.min(needed - liveWorkers, MAX_INSTANCES - liveInstances);
            if (toStart <= 0) {
                port.log("no workers to launch: " + needed + " needed, " + liveWorkers
                        + " live fleet-wide, instance cap at " + liveInstances + "/" + MAX_INSTANCES);
                return;
            }
            List<String> launched = launch(toStart);
            port.log("launched " + launched.size() + " worker(s): " + outstanding + " image(s), "
                    + workerFilesN + " per worker, " + liveWorkers + " were live fleet-wide, "
                    + liveInstances + "/" + MAX_INSTANCES + " instances before launch");
        } catch (Exception e) {
            // Worker bootstrap is best effort: the manager must keep serving clients.
            System.err.println("[MANAGER] could not launch workers: " + e.getMessage());
        }
    }

    // Launches workers and records their IDs as this manager's workers.
    List<String> launch(int count) {
        List<String> launched = port.launchWorkerInstances(count);
        workerInstanceIds.addAll(launched);
        return launched;
    }

    // Launches worker instances in AWS.
    List<String> launchInstances(int count) {
        AWS aws = AWS.getInstance();
        return aws.createEC2Instances(launchScript(aws.bucketName), WORKER_TAG, count);
    }

    // User-data script for one worker, from the worker-bootstrap.sh resource.
    String launchScript(String bucketName) {
        String script = loadTemplate();
        return script
                .replace("@DEBUG_LOG_UPLOAD@", Manager.debug ? DEBUG_LOG_UPLOAD : "")
                .replace("@BUCKET@", bucketName)
                .replace("@JAR_KEY@", WORKER_JAR_KEY)
                .replace("@JAR_PATH@", WORKER_JAR_PATH)
                .replace("@REGION@", AWS.region2.id());
    }

    private static String loadTemplate() {
        InputStream in = WorkerOrchestrator.class.getResourceAsStream("/worker-bootstrap.sh");
        if (in == null) {
            throw new IllegalStateException("worker-bootstrap.sh missing from the classpath");
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
            return new String(out.toByteArray(), "UTF-8");
        } catch (IOException e) {
            throw new IllegalStateException("cannot read worker-bootstrap.sh", e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }

    // Terminates only the workers this manager launched; never calls AWS in dev mode.
    void stop() {
        if (Queues.isDev()) {
            return;
        }
        List<String> ids = new ArrayList<String>(workerInstanceIds);
        if (ids.isEmpty()) {
            return;
        }
        try {
            port.terminateInstances(ids);
        } catch (Exception e) {
            // Worker shutdown is best effort and must not prevent the terminate acknowledgement.
            System.err.println("[MANAGER] could not stop workers: " + e.getMessage());
        } finally {
            workerInstanceIds.clear();
        }
    }

    // Drops workers that are gone or stopped and launches replacements for outstanding work.
    void reconcile() {
        if (port.isTerminating() || Queues.isDev()) {
            return;
        }
        try {
            if (!workerInstanceIds.isEmpty()) {
                Map<String, String> states = port.workerStates(workerInstanceIds);
                for (String id : new ArrayList<String>(workerInstanceIds)) {
                    String state = states.get(id);
                    if (state == null || isDeadWorkerState(state)) {
                        workerInstanceIds.remove(id);
                        port.log("removed dead worker " + id + " (state " + state + ")");
                    }
                }
            }
            port.ensureWorkers(port.totalOutstanding());
        } catch (Exception e) {
            // Replacement is best effort: a failed EC2 lookup must not stop the manager.
            System.err.println("[MANAGER] could not reconcile workers: " + e.getMessage());
        }
    }

    // EC2 states in which a worker can no longer process messages. A worker in "shutting-down"
    // may be a cap kill, not a crash, and still counts as live until EC2 removes it.
    private static boolean isDeadWorkerState(String state) {
        return state.equals("terminated")
                || state.equals("stopped");
    }
}
