package org.kendar.pfm.web.dto;

/** Per-tag totals. */
public record TagSummary(String tag, long in, long out, long net) {
}
