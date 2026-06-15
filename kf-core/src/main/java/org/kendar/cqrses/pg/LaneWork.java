package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.InternalMessage;

import java.util.List;

/** Queue item for a lane: the message plus the exact consumer registrations to
 *  invoke. Projection/aggregate lanes get the full list for the message type;
 *  saga lanes get a single-registration list chosen by the resolver. */
public record LaneWork(InternalMessage msg, List<Bus.Registration> consumers) {}
