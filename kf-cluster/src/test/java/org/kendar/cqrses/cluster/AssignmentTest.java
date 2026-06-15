package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentTest {

    @Test
    void balancesEvenlyFromScratch() {
        Map<Integer, String> target = Assignment.compute(9, List.of("A", "B", "C"), Map.of());
        assertEquals(9, target.size());
        assertEquals(3, count(target, "A"));
        assertEquals(3, count(target, "B"));
        assertEquals(3, count(target, "C"));
    }

    @Test
    void unevenSplitIsBalancedToFloorAndCeil() {
        Map<Integer, String> target = Assignment.compute(10, List.of("A", "B", "C"), Map.of());
        // 10/3 → counts in {3,4}; total 10.
        for (String m : List.of("A", "B", "C")) {
            int c = count(target, m);
            assertTrue(c == 3 || c == 4, m + " had " + c);
        }
        assertEquals(10, target.size());
    }

    @Test
    void addingANodeMovesOnlyTheNewNodesFairShare() {
        // 9 items evenly on A,B,C (3 each).
        Map<Integer, String> current = Assignment.compute(9, List.of("A", "B", "C"), Map.of());
        Map<Integer, String> after = Assignment.compute(9, List.of("A", "B", "C", "D"), current);

        // D should gain ~9/4 → 2 items; everyone else keeps the rest. Total moved == D's share.
        int moved = 0;
        for (int i = 0; i < 9; i++) {
            if (!current.get(i).equals(after.get(i))) {
                moved++;
                assertEquals("D", after.get(i), "only moves should land on the new node");
            }
        }
        assertEquals(count(after, "D"), moved);
        assertTrue(count(after, "D") >= 2 && count(after, "D") <= 3, "D fair share");
    }

    @Test
    void orphansFromDeadOwnerAreReassignedKeepingLiveOwners() {
        Map<Integer, String> current = new HashMap<>();
        current.put(0, "A");
        current.put(1, "A");
        current.put(2, "DEAD");
        current.put(3, "B");
        // DEAD is not a member → item 2 is orphaned and reassigned; A and B keep theirs.
        Map<Integer, String> after = Assignment.compute(4, List.of("A", "B"), current);
        assertEquals("A", after.get(0));
        assertEquals("A", after.get(1));
        assertEquals("B", after.get(3));
        assertTrue(after.get(2).equals("A") || after.get(2).equals("B"));
        assertEquals(2, count(after, "A"));
        assertEquals(2, count(after, "B"));
    }

    @Test
    void noMembersYieldsEmptyAssignment() {
        assertTrue(Assignment.compute(5, List.of(), Map.of()).isEmpty());
    }

    @Test
    void deterministicRegardlessOfMemberOrder() {
        Map<Integer, String> a = Assignment.compute(7, List.of("A", "B", "C"), Map.of());
        Map<Integer, String> b = Assignment.compute(7, List.of("C", "A", "B"), Map.of());
        assertEquals(a, b);
    }

    private static int count(Map<Integer, String> m, String v) {
        return (int) m.values().stream().filter(v::equals).count();
    }
}
