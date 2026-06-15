package org.kendar.cqrses.cluster.rows;

/**
 * A {@code cluster_assignments} row for one partition.
 * <p>
 * {@code ownerNode} is <b>leader-controlled</b> ("who SHOULD own"), {@code leaseHolder} is
 * <b>worker-controlled</b> ("who is processing NOW"). They are deliberately separate so a
 * handoff can flip the owner while the losing node keeps its lease alive — guaranteeing no
 * double-pump. {@code epoch} is the fencing epoch of the leader that last wrote the owner.
 * {@code leaseHolder} / {@code leaseUntil} are {@code null} when the partition is free.
 */
public record AssignmentRow(int itemId, String ownerNode, long epoch,
                            String leaseHolder, Long leaseUntil) {
}
