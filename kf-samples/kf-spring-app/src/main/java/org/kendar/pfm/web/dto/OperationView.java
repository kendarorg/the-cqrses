package org.kendar.pfm.web.dto;

/** A single operation as shown in the operations list. */
public record OperationView(String opId, String type, long amount, String tag, long ts) {
}
