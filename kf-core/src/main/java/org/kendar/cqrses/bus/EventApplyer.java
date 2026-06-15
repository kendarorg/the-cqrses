package org.kendar.cqrses.bus;

import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Single entry point for folding an event onto an aggregate instance.
 *
 * <p>Used from two places:
 * <ul>
 *   <li><b>Command flow</b> — inside an aggregate's {@code @CommandHandler},
 *       the aggregate calls {@code apply(this, event)} for every event it
 *       wants to emit. The call (a) invokes the aggregate's matching
 *       {@code @EventHandler} so in-memory state stays consistent and
 *       (b) records the event in the per-command buffer initialised by
 *       {@link #begin()} so the bus can persist + publish it after the
 *       handler returns successfully.</li>
 *   <li><b>Rehydrate</b> — {@code BaseEventStore.loadAggregate} folds the
 *       stored stream by calling {@code apply(instance, event)} with no
 *       active buffer; the fold path is identical but nothing is recorded.</li>
 * </ul>
 *
 * <p>The buffer lives in a {@link ThreadLocal} because the bus invokes the
 * command handler on its own worker thread and an aggregate has no other way
 * to reach the surrounding dispatch context without changing the
 * {@code @CommandHandler} method signature. {@code begin()} / {@code drain()}
 * are package-private — only the bus is allowed to bracket a command with
 * them. Until a real transactional outbox lands, append-to-store and
 * publish-to-bus run sequentially in the bus right after {@code drain()}.
 */
public class EventApplyer {

    private static final ThreadLocal<List<Object>> BUFFER = new ThreadLocal<>();

    private EventApplyer() {
    }

    /**
     * Folds {@code event} onto {@code aggregateInstance} by invoking the
     * aggregate's matching {@code @EventHandler} (resolved via
     * {@link GlobalRegistry#getEventHandler(Class, Object)}). If a command
     * buffer is active on the current thread, the event is also recorded
     * there for later persistence + publication.
     *
     * @throws InvalidRegistrationException if the aggregate has no
     *                                      {@code @EventHandler} for {@code event.getClass()}.
     */
    public static void apply(Object aggregateInstance, Object event) {
        apply(aggregateInstance, event, null);
    }

    /**
     * Three-arg overload used by the rehydrate path so {@code @EventHandler}
     * methods that declare a {@link Context} parameter receive the stored
     * event's context (aggregateId, version, traceId). Command-flow callers
     * pass {@code null} — aggregates aren't expected to read Context inside
     * an {@code @EventHandler} during a command.
     */
    public static void apply(Object aggregateInstance, Object event, Context context) {
        if (aggregateInstance == null) {
            throw new IllegalArgumentException("aggregateInstance must not be null");
        }
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        var handler = GlobalRegistry.getEventHandler(aggregateInstance.getClass(), event);
        if (handler == null) {
            throw new InvalidRegistrationException(
                    "Aggregate " + aggregateInstance.getClass().getName() +
                            " has no @EventHandler for " + event.getClass().getName());
        }
        handler.accept(aggregateInstance, event, context);
        var buf = BUFFER.get();
        if (buf != null) buf.add(event);
    }

    /**
     * Bus-only. Open a fresh buffer on the current thread for one command.
     */
    public static void begin() {
        BUFFER.set(new ArrayList<>());
    }

    /**
     * Bus-only. Return the events recorded since {@link #begin()} and clear.
     */
    public static List<Object> drain() {
        var buf = BUFFER.get();
        BUFFER.remove();
        return buf == null ? List.of() : buf;
    }

    /**
     * True iff a command buffer is active on the current thread.
     */
    static boolean isActive() {
        return BUFFER.get() != null;
    }
}
