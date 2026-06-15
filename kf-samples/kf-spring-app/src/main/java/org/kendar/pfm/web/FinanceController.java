package org.kendar.pfm.web;

import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kendar.pfm.domain.commands.RecordOperation;
import org.kendar.pfm.domain.commands.RegisterUser;
import org.kendar.pfm.read.OperationReadStore;
import org.kendar.pfm.read.UserReadStore;
import org.kendar.pfm.read.Uuids;
import org.kendar.pfm.web.dto.LoginRequest;
import org.kendar.pfm.web.dto.LoginResponse;
import org.kendar.pfm.web.dto.OperationRequest;
import org.kendar.pfm.web.dto.OperationResponse;
import org.kendar.pfm.web.dto.OperationView;
import org.kendar.pfm.web.dto.Summary;
import org.kendar.pfm.web.dto.TagSummary;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

/**
 * HTTP surface for the finance manager. Writes go through the auto-wired {@link CommandBus}
 * ({@code sendSync}); reads come straight from the durable read stores. There is no QueryBus in the
 * framework — projections are queried directly.
 */
@RestController
@RequestMapping("/api")
public class FinanceController {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinanceController.class);

    private final CommandBus commandBus;
    private final UserReadStore users;
    private final OperationReadStore operations;

    public FinanceController(CommandBus commandBus, UserReadStore users, OperationReadStore operations) {
        this.commandBus = commandBus;
        this.users = users;
        this.operations = operations;
    }

    /** Login with username only; register the user if not already known. */
    @PostMapping("/login")
    public LoginResponse login(@RequestBody LoginRequest req) {
        String username = require(req.username(), "username");
        UUID userId = Uuids.userId(username);
        boolean existed = users.exists(userId);
        if (!existed) {
            commandBus.sendSync(new RegisterUser(userId, username));
        }
        return new LoginResponse(userId.toString(), username, !existed);
    }

    @PostMapping("/operations")
    public OperationResponse record(@RequestBody OperationRequest req) {
        String username = require(req.username(), "username");
        if (req.type() == null) throw badRequest("type is required (IN or OUT)");
        if (req.amount() <= 0) throw badRequest("amount must be positive");
        String tag = require(req.tag(), "tag");
        UUID userId = Uuids.userId(username);
        UUID opId = UUIDGenerator.newUuid();
        commandBus.sendSync(new RecordOperation(userId, opId, req.type(), req.amount(), tag));
        LOGGER.trace("recorded op user={} userId={} seg={} type={} amount={} tag={} opId={}",
                username, userId, SegmentCalculator.calculateSegment(userId), req.type(), req.amount(),
                tag, opId);
        return new OperationResponse(opId.toString());
    }

    @GetMapping("/summary")
    public Summary summary(@RequestParam String username) {
        return operations.summary(Uuids.userId(require(username, "username")));
    }

    @GetMapping("/summary/by-tag")
    public List<TagSummary> byTag(@RequestParam String username) {
        return operations.byTag(Uuids.userId(require(username, "username")));
    }

    @GetMapping("/operations")
    public List<OperationView> list(@RequestParam String username) {
        return operations.recent(Uuids.userId(require(username, "username")), 50);
    }

    private static String require(String value, String name) {
        if (value == null || value.isBlank()) throw badRequest(name + " is required");
        return value.trim();
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(BAD_REQUEST, message);
    }
}
