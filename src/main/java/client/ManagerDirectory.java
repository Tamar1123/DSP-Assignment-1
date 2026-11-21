package client;

import java.util.List;

/** Where the client finds a manager. Faked in tests; the real one is {@link ManagerFinder}. */
interface ManagerDirectory {

    /** IDs of running managers that reported at least one free client slot. */
    List<String> freeManagers();

    /** Starts one manager with {@code freeSlots} free client slots (best effort). */
    void startManager(int freeSlots);

    /**
     * Makes sure some manager can take a client: keeps a free one, else starts a new one.
     *
     * @return true if a new manager was started
     */
    default boolean ensureManager(int freeSlots) {
        List<String> free = freeManagers();
        if (free != null && !free.isEmpty()) {
            return false;
        }
        startManager(freeSlots);
        return true;
    }

    /** Whether at least one manager instance is currently alive (running or pending). */
    default boolean hasLiveManager() {
        return true;
    }
}
