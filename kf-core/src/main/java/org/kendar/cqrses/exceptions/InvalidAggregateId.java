package org.kendar.cqrses.exceptions;

public class InvalidAggregateId extends RuntimeException {
    public InvalidAggregateId(String message) {
        super(message);
    }
}
