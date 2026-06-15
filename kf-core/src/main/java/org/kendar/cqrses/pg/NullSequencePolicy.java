package org.kendar.cqrses.pg;

public class NullSequencePolicy implements SequencePolicy {
    @Override
    public String getSequenceId(Object eventCommand) {
        return "NullSequencePolicy";
    }
}
