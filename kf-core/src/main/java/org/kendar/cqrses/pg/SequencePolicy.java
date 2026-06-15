package org.kendar.cqrses.pg;

public interface SequencePolicy {
    String getSequenceId(Object eventCommand);
}
