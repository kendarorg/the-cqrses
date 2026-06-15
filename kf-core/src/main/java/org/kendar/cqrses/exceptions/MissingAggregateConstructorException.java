package org.kendar.cqrses.exceptions;

public class MissingAggregateConstructorException extends RuntimeException {
    public MissingAggregateConstructorException(String message, Throwable cause) {

        super(message, cause);
    }
}
