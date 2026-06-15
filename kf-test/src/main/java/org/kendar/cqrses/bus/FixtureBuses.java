package org.kendar.cqrses.bus;

import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Bus doubles for the test fixtures. They live in {@code org.kendar.cqrses.bus}
 * because the {@code send}/{@code sendSync} template methods are package-private
 * — the same reason kf-core's own test {@code StubBuses} sits in this package.
 */
public final class FixtureBuses {

    private FixtureBuses() {
    }

    /**
     * Command bus that records every dispatched command instead of handling it.
     * {@code SagaTestFixture} registers it as the {@code CommandBus} so a saga's
     * {@code GlobalRegistry.get(CommandBus.class).send(...)} side effects become
     * observable assertions instead of going anywhere.
     */
    public static class RecordingCommandBus extends CommandBus {
        private final List<Object> recorded = new CopyOnWriteArrayList<>();

        public RecordingCommandBus(MessageSerializer serializer) {
            super(serializer, null);
        }

        public List<Object> recorded() {
            return recorded;
        }

        public void reset() {
            recorded.clear();
        }

        @Override
        public Object findTarget(Object command, Registration registration) {
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void clear() {
            recorded.clear();
        }

        @Override
        void send(Object command, Context context) {
            recorded.add(command);
        }

        @Override
        Object sendSync(Object command, Context context) {
            recorded.add(command);
            return null;
        }

        @Override
        protected boolean registerInternal(Class<?> handlerClass) {
            return false;
        }
    }

    /**
     * No-op event bus so {@code AggregateTestFixture}'s command pipeline can
     * "publish" emitted events without an event side: the store append (what the
     * fixture asserts on) has already happened by the time publish runs.
     */
    public static EventBus noopEventBus(MessageSerializer serializer, SagaStore sagaStore) {
        return new EventBus(serializer, sagaStore) {
            @Override
            public Object findTarget(Object event, Registration registration) {
                return null;
            }

            @Override
            public void start() {
            }

            @Override
            public void stop() {
            }

            @Override
            public void clear() {
            }

            @Override
            void send(Object event, Context context) {
            }

            @Override
            protected boolean registerInternal(Class<?> handlerClass) {
                return false;
            }
        };
    }
}
