package org.kendar.pfm.web.dto;

import org.kendar.pfm.domain.OpType;

public record OperationRequest(String username, OpType type, long amount, String tag) {
}
