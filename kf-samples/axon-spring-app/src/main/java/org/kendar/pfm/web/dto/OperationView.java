package org.kendar.pfm.web.dto;

public record OperationView(String opId, String type, long amount, String tag, long ts) {
}
