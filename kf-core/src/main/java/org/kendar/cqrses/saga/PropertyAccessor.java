package org.kendar.cqrses.saga;

public interface PropertyAccessor {
    Object get(Object target, String property);
}
