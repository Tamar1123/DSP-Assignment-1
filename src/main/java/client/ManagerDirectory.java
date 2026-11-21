package client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// Where the client finds a manager. Faked in tests; the real one is ManagerFinder.
interface ManagerDirectory {

    // IDs of running managers that reported at least one free client slot, most free first.
    List<String> freeManagers();

    // Starts one manager with freeSlots free client slots (best effort); returns its instance id.
    String startManager(int freeSlots);

    // Picks the manager this client will talk to: the first free one not in avoid, else a newly
    // started one. Sets started[0] when a manager was launched.
    default String ensureManager(int freeSlots, Set<String> avoid, boolean[] started) {
        List<String> free = freeManagers();
        if (free == null) {
            free = new ArrayList<String>();
        }
        for (String id : free) {
            if (avoid == null || !avoid.contains(id)) {
                return id;
            }
        }
        if (started != null && started.length > 0) {
            started[0] = true;
        }
        return startManager(freeSlots);
    }

    // Whether at least one manager instance is currently alive (running or pending).
    default boolean hasLiveManager() {
        return true;
    }
}
