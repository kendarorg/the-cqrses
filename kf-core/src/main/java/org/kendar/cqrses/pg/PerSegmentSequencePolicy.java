package org.kendar.cqrses.pg;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Scopes the sequence to {@code (group, segment(aggregateId))}: per-segment
 * blocking blocks exactly that lane. Constructed bare; the framework stamps the
 * group from the handler annotation at registration (see
 * {@code Bus.setProcessingGroupPolicy}). Must NOT be used on saga groups — their
 * lane is {@code segment(sagaId)}, invisible to {@code getSequenceId(event)}.
 */
public class PerSegmentSequencePolicy implements SequencePolicy {
    private String group = "default";

    public void setGroup(String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public String getSequenceId(Object eventCommand) {
        if (eventCommand == null) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in null event/command");
        }
        List<Field> fields = ReflectionUtils.getFieldsAnnotatedWith(
                eventCommand.getClass(), AggregateIdentifier.class);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
        }
        try {
            Object value = fields.get(0).get(eventCommand);
            if (value == null) {
                throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
            }
            var tsValue = value.toString().trim();
            if (tsValue.isEmpty()) {
                throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
            }
            return group + ":seg:" + SegmentCalculator.calculateSegment(tsValue);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
        }
    }
}
