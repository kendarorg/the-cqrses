package org.kendar.cqrses.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure minimal-movement partition assignment.
 * <p>
 * Keeps the current owner of a partition if that owner is still a live member; reassigns only
 * <i>orphaned</i> partitions (owner dead/unknown) and the fair share that must move when
 * {@code liveCount} changes, balancing toward {@code ~N/liveCount} per node. A 3→4 node change
 * moves only the new node's fair share (existing owners are otherwise untouched) — the opposite of
 * {@code i mod liveCount}, which would remap almost every partition on a single membership change.
 */
final class Assignment {

    private Assignment() {
    }

    /**
     * @param n       partition count (items {@code 0..n-1})
     * @param members live node ids (order irrelevant; method is deterministic regardless)
     * @param current current owner per item ({@code null} / absent = unowned)
     * @return target owner per item; empty if there are no members (nothing can be assigned)
     */
    static Map<Integer, String> compute(int n, List<String> members, Map<Integer, String> current) {
        Map<Integer, String> target = new LinkedHashMap<>();
        if (members.isEmpty()) {
            return target;
        }
        List<String> sortedMembers = new ArrayList<>(members);
        sortedMembers.sort(Comparator.naturalOrder());
        int count = sortedMembers.size();

        // Current valid holdings: item -> owner, only where owner is a live member.
        Map<String, List<Integer>> holdings = new HashMap<>();
        for (String m : sortedMembers) {
            holdings.put(m, new ArrayList<>());
        }
        List<Integer> orphans = new ArrayList<>();
        for (int item = 0; item < n; item++) {
            String owner = current.get(item);
            if (owner != null && holdings.containsKey(owner)) {
                holdings.get(owner).add(item);
            } else {
                orphans.add(item);
            }
        }

        int base = n / count;
        int rem = n % count;
        // Capacity: rem members get base+1, the rest base. To minimise movement, the +1 slots go
        // to the members already carrying the most, so heavily-loaded nodes shed the least.
        List<String> byLoadDesc = new ArrayList<>(sortedMembers);
        byLoadDesc.sort(Comparator
                .comparingInt((String m) -> holdings.get(m).size()).reversed()
                .thenComparing(Comparator.naturalOrder()));
        Map<String, Integer> capacity = new HashMap<>();
        for (int i = 0; i < byLoadDesc.size(); i++) {
            capacity.put(byLoadDesc.get(i), i < rem ? base + 1 : base);
        }

        // Shed over-capacity items back into the orphan pool (deterministic: highest item ids first).
        for (String m : sortedMembers) {
            List<Integer> held = holdings.get(m);
            int cap = capacity.get(m);
            held.sort(Comparator.naturalOrder());
            while (held.size() > cap) {
                orphans.add(held.remove(held.size() - 1));
            }
        }

        // Fill under-capacity members from the orphan pool, least-loaded first (so a brand-new node
        // — load 0 — fills before nodes already near capacity).
        orphans.sort(Comparator.naturalOrder());
        int oi = 0;
        boolean progressed = true;
        while (oi < orphans.size() && progressed) {
            progressed = false;
            List<String> byRoom = new ArrayList<>(sortedMembers);
            byRoom.sort(Comparator
                    .comparingInt((String m) -> holdings.get(m).size())
                    .thenComparing(Comparator.naturalOrder()));
            for (String m : byRoom) {
                if (oi >= orphans.size()) {
                    break;
                }
                if (holdings.get(m).size() < capacity.get(m)) {
                    holdings.get(m).add(orphans.get(oi++));
                    progressed = true;
                }
            }
        }

        for (Map.Entry<String, List<Integer>> e : holdings.entrySet()) {
            for (Integer item : e.getValue()) {
                target.put(item, e.getKey());
            }
        }
        return target;
    }
}
