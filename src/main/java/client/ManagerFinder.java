package client;

import utils.AWS;
import utils.Queues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Finds a free manager on EC2 (managers tag themselves with their free slots) or launches one.
 * The dev transport has no EC2, so there it finds nothing and starts nothing.
 */
final class ManagerFinder implements ManagerDirectory {

    @Override
    public List<String> freeManagers() {
        if (Queues.isDev()) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<String>();
        for (AWS.ManagerSlot slot : AWS.getInstance().freeManagers()) {
            if (slot != null && slot.free > 0) {
                ids.add(slot.id);
            }
        }
        return ids;
    }

    @Override
    public boolean hasLiveManager() {
        if (Queues.isDev()) {
            return true;
        }
        return AWS.getInstance().liveManagerCount() > 0;
    }

    @Override
    public void startManager(int freeSlots) {
        if (Queues.isDev()) {
            return;
        }
        // The launch carries the free-slot tag, so other clients see it and don't start another.
        // App owns the boot script: fetch the uploaded Manager jar and run it.
        App.createEC2(freeSlots);
    }
}
