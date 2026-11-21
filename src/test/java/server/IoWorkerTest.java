package server;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IoWorkerTest {
    private static IoWorker.Job job(final List<String> events, final String name, int priority) {
        return new IoWorker.Job(priority) {
            @Override
            public void run() {
                events.add(name);
            }
        };
    }

    @Test
    public void finishesRunBeforeFanOuts() {
        List<String> events = new ArrayList<String>();
        IoWorker worker = new IoWorker("test-io");
        worker.submit(job(events, "fan", IoWorker.FAN_OUT));
        worker.submit(job(events, "finish", IoWorker.FINISH));

        worker.runAll();

        assertEquals(Arrays.asList("finish", "fan"), events);
    }

    @Test
    public void samePriorityKeepsSubmissionOrder() {
        List<String> events = new ArrayList<String>();
        IoWorker worker = new IoWorker("test-io");
        worker.submit(job(events, "first", IoWorker.FAN_OUT));
        worker.submit(job(events, "second", IoWorker.FAN_OUT));

        worker.runAll();

        assertEquals(Arrays.asList("first", "second"), events);
    }

    @Test
    public void runPendingFinishesLeavesFanOutsQueued() {
        List<String> events = new ArrayList<String>();
        IoWorker worker = new IoWorker("test-io");
        worker.submit(job(events, "fan", IoWorker.FAN_OUT));
        worker.submit(job(events, "finish", IoWorker.FINISH));

        worker.runPendingFinishes();
        assertEquals(Arrays.asList("finish"), events);

        worker.runAll();
        assertEquals(Arrays.asList("finish", "fan"), events);
    }

    @Test
    public void stopIsNotRunAsWork() {
        IoWorker worker = new IoWorker("test-io");
        worker.stopAfterDrain();
        assertFalse(worker.runOne());
    }
}
