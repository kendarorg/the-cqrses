package org.kendar.cqrses.serialization;

import org.kendar.cqrses.annotations.UpcasterSpec;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.exceptions.SerializerException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Read-path event upcaster. Walks a stored {@link InternalMessage} from whatever schema
 * version it was persisted at up to the latest, by chaining one-hop transforms declared
 * with {@link UpcasterSpec}.
 *
 * <p>Hops are discovered once, at construction (setup phase), and indexed by origin type
 * name (lower-cased — matching is case-insensitive, simple-name only). At runtime
 * {@link #upcast(InternalMessage)} runs a fixpoint loop: while some hop matches the
 * message's current {@code (type, version)} it applies the <em>widest</em> one, advances
 * {@code Context.version} (and {@code Context.type} on a rename), and repeats. Versions
 * strictly increase each step, so the loop always terminates.
 *
 * <p>The transform operates on the serialized <em>tree</em> ({@code JsonNode} via
 * {@link JacksonMessageSerializer#toNode}/{@link JacksonMessageSerializer#fromNode}), never
 * on a deserialized object — the origin class is never required and may have been deleted.
 */
public class UpcastersManager {
    private MessageSerializer serializer;


    /** One declared {@code (origin, from) -> (to, target)} transform plus its backing method. */
    private static final class Hop {
        final int from;
        final int to;          // effective target version, always > from
        final String target;   // "" = leave Context.type unchanged
        final Object owner;
        final Method method;

        Hop(int from, int to, String target, Object owner, Method method) {
            this.from = from;
            this.to = to;
            this.target = target;
            this.owner = owner;
            this.method = method;
        }
    }

    // lower-cased origin type name -> hops declared for that type
    private final Map<String, List<Hop>> hopsByType = new HashMap<>();
    private final boolean empty;

    public UpcastersManager(MessageSerializer serializer,List<Upcaster> upcasters) {
        this.serializer = serializer;
        for (Upcaster upcaster : upcasters) {
            for (Method m : upcaster.getClass().getMethods()) {
                UpcasterSpec spec = m.getAnnotation(UpcasterSpec.class);
                if (spec == null) continue;
                validateSignature(m);

                int from = spec.from();
                int to = spec.to() == 0 ? from + 1 : spec.to();
                if (to <= from) {
                    throw new IllegalStateException(
                            "@UpcasterSpec on " + m + " declares to=" + to + " <= from=" + from
                                    + "; an upcaster must advance the version");
                }

                m.setAccessible(true);
                String key = spec.origin().toLowerCase(Locale.ROOT);
                List<Hop> hops = hopsByType.computeIfAbsent(key, k -> new ArrayList<>());
                for (Hop existing : hops) {
                    if (existing.from == from && existing.to == to) {
                        throw new IllegalStateException(
                                "Duplicate @UpcasterSpec for origin '" + spec.origin()
                                        + "' from=" + from + " to=" + to
                                        + " — ambiguous widest-hop selection");
                    }
                }
                hops.add(new Hop(from, to, spec.target(), upcaster, m));
            }
        }
        this.empty = hopsByType.isEmpty();
    }

    public InternalMessage upcast(InternalMessage msg) {
        if (empty) return msg;
        Context ctx = msg.getContext();
        if (ctx == null || ctx.getType() == null) return msg;

        byte[] payload = msg.getPayload();
        boolean changed = false;

        while (true) {
            List<Hop> hops = hopsByType.get(ctx.getType().toLowerCase(Locale.ROOT));
            if (hops == null) break;

            Hop chosen = null;
            for (Hop h : hops) {
                if (h.from != ctx.getVersion()) continue;
                if (chosen == null || h.to > chosen.to) chosen = h; // widest jump wins
            }
            if (chosen == null) break;

            payload = applyHop(chosen, payload);
            ctx.setVersion(chosen.to);
            if (!chosen.target.isEmpty()) ctx.setType(chosen.target);
            changed = true;
        }

        if (changed) msg.setPayload(payload);
        return msg;
    }

    private byte[] applyHop(Hop hop, byte[] payload) {

        try {
            var node = serializer.deserializeToIntermediate(payload);
            Object upcasted = hop.method.invoke(hop.owner, node);
            return serializer.serialize(upcasted);
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause() != null ? ite.getCause() : ite;
            throw new SerializerException(
                    cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
        } catch (Exception ex) {
            throw new SerializerException(ex);
        }
    }

    private static void validateSignature(Method m) {
        if (m.getParameterCount() != 1
                || !Object.class.isAssignableFrom(m.getParameterTypes()[0])
                || !Object.class.isAssignableFrom(m.getReturnType())) {
            throw new IllegalStateException(
                    "@UpcasterSpec method " + m + " must have signature (JsonNode) -> JsonNode");
        }
    }
}
