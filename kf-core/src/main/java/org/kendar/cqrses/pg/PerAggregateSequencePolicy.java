package org.kendar.cqrses.pg;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

public class PerAggregateSequencePolicy implements SequencePolicy {
    @Override
    public String getSequenceId(Object eventCommand) {
        if (eventCommand == null) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
        }
        List<Field> fields = ReflectionUtils.getFieldsAnnotatedWith(
                eventCommand.getClass(), AggregateIdentifier.class);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
        }
        try {
            Object value = fields.get(0).get(eventCommand);
            if(value == null){
                throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
            }
            var tsValue = value.toString().trim();
            if(tsValue.isEmpty()){
                throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
            }
            return tsValue;
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("No @AggregateIdentifier field found in " + eventCommand.getClass().getName());
        }
    }
}
