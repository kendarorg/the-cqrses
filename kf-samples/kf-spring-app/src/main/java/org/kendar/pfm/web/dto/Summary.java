package org.kendar.pfm.web.dto;

/** Totals across all of a user's operations. {@code net = in - out}. */
public record Summary(long in, long out, long net) {
}
