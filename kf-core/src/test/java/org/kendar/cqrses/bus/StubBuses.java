package org.kendar.cqrses.bus;

/**
 * Test stubs for the abstract Bus subclasses — used by tests that need a bus
 * registered in {@link org.kendar.cqrses.di.GlobalRegistry} so autoSubscribe()
 * does not NPE. The {@code send}/{@code sendSync} overrides have package
 * visibility, which is why this lives in {@code org.kendar.cqrses.bus}.
 */
public class StubBuses {

    private StubBuses() {
    }

    public static CommandBus noopCommandBus() {
        return new CommandBus(null, null) {
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
            }

            @Override
            void send(Object command, Context context) {
            }

            @Override
            Object sendSync(Object command, Context context) {
                return null;
            }

            @Override
            protected boolean registerInternal(Class<?> handlerClass) {
                return false;
            }
        };
    }

    public static EventBus noopEventBus() {
        return new EventBus(null, null) {
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
            void send(Object command, Context context) {
            }

            @Override
            protected boolean registerInternal(Class<?> handlerClass) {
                return false;
            }
        };
    }
}
