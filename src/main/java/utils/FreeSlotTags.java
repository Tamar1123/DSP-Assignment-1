package utils;

import java.util.List;
import java.util.Map;

// Pure parsing and selection logic for manager free-slot tags; no AWS dependencies.
public final class FreeSlotTags {
    public static final String FREE_TAG = "free";
    public static final int UNKNOWN_FREE = -1;

    private FreeSlotTags() {
    }

    // One manager instance and its reported free-slot hint.
    public static class ManagerSlot {
        public final String id;
        public final int free;

        public ManagerSlot(String id, int free) {
            this.id = id;
            this.free = free;
        }

        @Override
        public String toString() {
            return id + "(free=" + free + ")";
        }
    }

    // Serializes a free count for the tag; a negative count means "full".
    public static String formatFreeSlots(int free) {
        return Integer.toString(Math.max(0, free));
    }

    // Reads a free count; unknown for null, blank, non-numeric, or negative input.
    public static int parseFreeSlots(String raw) {
        if (raw == null) {
            return UNKNOWN_FREE;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return UNKNOWN_FREE;
        }
        try {
            int free = Integer.parseInt(value);
            return free < 0 ? UNKNOWN_FREE : free;
        } catch (NumberFormatException e) {
            return UNKNOWN_FREE;
        }
    }

    // Free count from a tag map; missing or malformed tag yields UNKNOWN_FREE.
    public static int freeSlotsOf(Map<String, String> tags) {
        return tags == null ? UNKNOWN_FREE : parseFreeSlots(tags.get(FREE_TAG));
    }

    // Picks the most-free manager; null if no entry has a positive free count.
    public static ManagerSlot chooseFreeManager(List<? extends ManagerSlot> slots) {
        if (slots == null) {
            return null;
        }
        ManagerSlot best = null;
        for (ManagerSlot slot : slots) {
            if (slot == null || slot.free <= 0) {
                continue;
            }
            if (best == null || slot.free > best.free) {
                best = slot;
            }
        }
        return best;
    }
}
