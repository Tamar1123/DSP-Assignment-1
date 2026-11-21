package server;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// The clients one manager is serving, bounded by manager-clients-n, plus its counters.
class ClientRegistry {
    private final int maxClients;
    // Admitted clients keyed by localAppID; its size is the load.
    private final Map<String, ClientTask> tasks = new ConcurrentHashMap<String, ClientTask>();
    private final AtomicInteger served = new AtomicInteger(); // finished clients
    private final AtomicInteger rejected = new AtomicInteger(); // full-manager requeues

    ClientRegistry(int maxClients) {
        this.maxClients = maxClients;
    }

    boolean hasCapacity() { return tasks.size() < maxClients; }

    int maxClients() { return maxClients; }

    int freeSlots() { return maxClients - tasks.size(); }

    int size() { return tasks.size(); }

    boolean isEmpty() { return tasks.isEmpty(); }

    boolean contains(String localAppID) { return tasks.containsKey(localAppID); }

    ClientTask get(String localAppID) { return tasks.get(localAppID); }

    void add(ClientTask task) { tasks.put(task.localAppID, task); }

    void remove(ClientTask task) { tasks.remove(task.localAppID, task); }

    Collection<ClientTask> all() { return tasks.values(); }

    int served() { return served.get(); }

    int rejected() { return rejected.get(); }

    void markServed() { served.incrementAndGet(); }

    void markRejected() { rejected.incrementAndGet(); }

    // Image tasks still unanswered across all clients.
    int totalOutstanding() {
        int total = 0;
        for (ClientTask task : tasks.values()) {
            if (task.outstanding.get() > 0) {
                total += task.outstanding.get();
            }
        }
        return total;
    }
}
