package server;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

// One thread for all blocking manager work. Finishes run before fan-outs; same-priority jobs
// keep submission order. Tests can run jobs on the calling thread instead of starting it.
final class IoWorker {
    static final int FINISH = 0;
    static final int FAN_OUT = 1;
    private static final int STOP_PRIORITY = Integer.MAX_VALUE;

    // A blocking unit of work.
    abstract static class Job implements Runnable, Comparable<Job> {
        private static final AtomicLong SEQUENCE = new AtomicLong();

        private final int priority;
        private final long sequence = SEQUENCE.getAndIncrement();

        Job(int priority) {
            this.priority = priority;
        }

        int priority() {
            return priority;
        }

        @Override
        public int compareTo(Job other) {
            int byPriority = Integer.compare(priority, other.priority);
            return byPriority != 0 ? byPriority : Long.compare(sequence, other.sequence);
        }
    }

    private static final Job STOP = new Job(STOP_PRIORITY) {
        @Override
        public void run() {
        }
    };

    private final PriorityBlockingQueue<Job> jobs = new PriorityBlockingQueue<Job>();
    private final String threadName;

    IoWorker(String threadName) {
        this.threadName = threadName;
    }

    void submit(Job job) {
        jobs.add(job);
    }

    // Starts the worker thread.
    void start() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                loop();
            }
        }, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    // Stops after all queued jobs run.
    void stopAfterDrain() {
        jobs.add(STOP);
    }

    // Test seam: runs one job; false when empty or stopped.
    boolean runOne() {
        Job job = jobs.poll();
        if (job == null || job == STOP) {
            return false;
        }
        run(job);
        return true;
    }

    // Test seam: runs every queued job, finishes first.
    void runAll() {
        Job job;
        while ((job = jobs.poll()) != null) {
            if (job == STOP) {
                return;
            }
            run(job);
        }
    }

    // Runs queued finishes now; fan-out calls this between batches.
    void runPendingFinishes() {
        Job job;
        while ((job = jobs.peek()) != null && job != STOP && job.priority() == FINISH) {
            if (jobs.poll() != job) {
                break;
            }
            run(job);
        }
    }

    private void loop() {
        while (true) {
            Job job;
            try {
                job = jobs.take();
            } catch (InterruptedException e) {
                return;
            }
            if (job == STOP) {
                return;
            }
            run(job);
        }
    }

    private void run(Job job) {
        try {
            job.run();
        } catch (Exception e) {
            // One bad job must not kill the worker; the job reports client failures.
            System.err.println("[MANAGER] io job failed: " + e.getMessage());
        }
    }
}
