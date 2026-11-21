package utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Pure free-slot tag logic; these tests must not construct AWS clients. */
public class AwsTagLogicTest {

    @Test
    public void formatsFreeCount() {
        assertEquals("3", FreeSlotTags.formatFreeSlots(3));
        assertEquals("0", FreeSlotTags.formatFreeSlots(0));
        assertEquals("0", FreeSlotTags.formatFreeSlots(-5));
    }

    @Test
    public void parsesFreeCountTolerantly() {
        assertEquals(3, FreeSlotTags.parseFreeSlots("3"));
        assertEquals(0, FreeSlotTags.parseFreeSlots("0"));
        assertEquals(12, FreeSlotTags.parseFreeSlots(" 12 "));
        assertEquals(-1, FreeSlotTags.parseFreeSlots(null));
        assertEquals(-1, FreeSlotTags.parseFreeSlots(""));
        assertEquals(-1, FreeSlotTags.parseFreeSlots("   "));
        assertEquals(-1, FreeSlotTags.parseFreeSlots("lots"));
        assertEquals(-1, FreeSlotTags.parseFreeSlots("2.5"));
        assertEquals(-1, FreeSlotTags.parseFreeSlots("-2"));
        assertEquals(-1, FreeSlotTags.parseFreeSlots("999999999999999999999"));
    }

    @Test
    public void tagMapWithoutAUsableFreeTagIsUnknown() {
        Map<String, String> tags = new HashMap<>();
        tags.put("Name", "manager");
        assertEquals(-1, FreeSlotTags.freeSlotsOf(tags));
        assertEquals(-1, FreeSlotTags.freeSlotsOf(null));

        tags.put("free", "lots");
        assertEquals(-1, FreeSlotTags.freeSlotsOf(tags));

        tags.put("free", "2");
        assertEquals(2, FreeSlotTags.freeSlotsOf(tags));
    }

    @Test
    public void choosesTheManagerWithMostFreeSlots() {
        List<FreeSlotTags.ManagerSlot> slots = Arrays.asList(
                new FreeSlotTags.ManagerSlot("i-1", 1),
                new FreeSlotTags.ManagerSlot("i-2", 3),
                new FreeSlotTags.ManagerSlot("i-3", 2));
        assertEquals("i-2", FreeSlotTags.chooseFreeManager(slots).id);
        assertEquals(3, FreeSlotTags.chooseFreeManager(slots).free);
    }

    @Test
    public void ignoresFullMalformedAndNullEntries() {
        List<FreeSlotTags.ManagerSlot> slots = new ArrayList<>();
        slots.add(null);
        slots.add(new FreeSlotTags.ManagerSlot("i-full", 0));
        slots.add(new FreeSlotTags.ManagerSlot("i-bad", -1));
        slots.add(new FreeSlotTags.ManagerSlot("i-ok", 4));
        assertEquals("i-ok", FreeSlotTags.chooseFreeManager(slots).id);
    }

    @Test
    public void instanceTagsCarryNameAndOnlyAUsableFreeHint() {
        // The unknown sentinel drops the free tag; a real count adds it.
        Map<String, String> worker = tagMap(AWS.buildInstanceTags("worker", AWS.UNKNOWN_FREE));
        assertEquals("worker", worker.get("Name"));
        assertFalse(worker.containsKey(AWS.FREE_TAG));

        Map<String, String> manager = tagMap(AWS.buildInstanceTags("manager", 3));
        assertEquals("manager", manager.get("Name"));
        assertEquals("3", manager.get(AWS.FREE_TAG));
    }

    private static Map<String, String> tagMap(List<software.amazon.awssdk.services.ec2.model.Tag> tags) {
        Map<String, String> map = new HashMap<>();
        for (software.amazon.awssdk.services.ec2.model.Tag tag : tags) {
            map.put(tag.key(), tag.value());
        }
        return map;
    }

    @Test
    public void noUsableManagerGivesNull() {
        assertNull(FreeSlotTags.chooseFreeManager(null));
        assertNull(FreeSlotTags.chooseFreeManager(Collections.<FreeSlotTags.ManagerSlot>emptyList()));
        assertNull(FreeSlotTags.chooseFreeManager(Arrays.asList(
                new FreeSlotTags.ManagerSlot("i-1", 0),
                new FreeSlotTags.ManagerSlot("i-2", -1))));
    }
}
