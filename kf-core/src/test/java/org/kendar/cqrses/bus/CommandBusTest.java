package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;
import org.kendar.cqrses.annotations.CommandInterceptor;
import org.kendar.cqrses.cluster.spi.CommandForwarder;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.RemoteCommandException;
import org.kendar.cqrses.repositories.AggregateSnapshot;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CommandBusTest {

    private CapturingCommandBus bus;
    private StubEventStore eventStore;
    private JacksonMessageSerializer serializer;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        CommandForwarding.reset();
        eventStore = new StubEventStore();
        serializer = new JacksonMessageSerializer();
        bus = new CapturingCommandBus(serializer, eventStore);
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
        CommandForwarding.reset();
    }

    @Test
    void constructorStoresSerializerAndEventStore() {
        assertSame(serializer, bus.serializer);
        assertSame(eventStore, bus.eventStore);
    }

    @Test
    void sendWithoutVersionUsesMinusOneAggregateVersion() {
        UUID id = UUIDGenerator.newUuid();
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = id;
        bus.send(cmd);

        assertEquals(1, bus.sendContexts.size());
        Context ctx = bus.sendContexts.get(0);
        assertEquals(id, ctx.getAggregateId());
        assertEquals(-1L, ctx.getAggregateVersion());
        assertEquals("VersionedCmd", ctx.getType());
        assertEquals(3L, ctx.getVersion());
        assertNotNull(ctx.getTraceId());
    }

    @Test
    void sendWithExplicitVersionPropagatesIt() {
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd, 9);
        assertEquals(9L, bus.sendContexts.get(0).getAggregateVersion());
    }

    @Test
    void sendDefaultVersionAnnotationReadsAsOne() {
        DefaultVersionCmd cmd = new DefaultVersionCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd);
        assertEquals(1L, bus.sendContexts.get(0).getVersion());
    }

    @Test
    void sendForwardsCommandInstanceUnchanged() {
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd);
        assertSame(cmd, bus.sendCommands.get(0));
    }

    @Test
    void sendSyncBuildsSameContextShape() {
        UUID id = UUIDGenerator.newUuid();
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = id;
        bus.sendSync(cmd, 4);

        Context ctx = bus.sendSyncContexts.get(0);
        assertEquals(id, ctx.getAggregateId());
        assertEquals(4L, ctx.getAggregateVersion());
        assertEquals("VersionedCmd", ctx.getType());
        assertEquals(3L, ctx.getVersion());
        assertNotNull(ctx.getTraceId());
    }

    @Test
    void sendSyncDefaultVersionAlsoMinusOne() {
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.sendSync(cmd);
        assertEquals(-1L, bus.sendSyncContexts.get(0).getAggregateVersion());
    }

    @Test
    void sendGivesUniqueTraceIdPerCall() {
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd);
        bus.send(cmd);
        assertNotEquals(bus.sendContexts.get(0).getTraceId(),
                bus.sendContexts.get(1).getTraceId());
    }

    @Test
    void registerInternalReturnsFalseForPlainClass() {
        bus.register(PlainClass.class);
        assertFalse(bus.subscribedClasses.contains(PlainClass.class));
    }

    @Test
    void registerInternalReturnsFalseForSagaClass() {
        bus.register(SagaSideClass.class);
        assertFalse(bus.subscribedClasses.contains(SagaSideClass.class));
    }

    @Test
    void registerInternalReturnsTrueForAggregateClass() {
        bus.register(AggregateClass.class);
        assertTrue(bus.subscribedClasses.contains(AggregateClass.class));
    }

    @Test
    void registerInternalReturnsTrueForInterceptorClass() {
        bus.register(InterceptorClass.class);
        assertTrue(bus.subscribedClasses.contains(InterceptorClass.class));
    }

    @Test
    void rehydrateAggregateStubReturnsNull() throws Exception {
        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        var method = Object.class.getDeclaredMethod("toString");
        var registration = new Bus.Registration(AggregateClass.class, null, null, method);
        assertNull(bus.rehydrateAggregate(cmd, registration));
    }

    // --- @AggregateIdentifier registration validation ---------------------------

    @Test
    void aggregateHandlingCommandWithUuidIdRegisters() {
        bus.register(AggregateWithGoodCommand.class);
        assertTrue(bus.subscribedClasses.contains(AggregateWithGoodCommand.class));
    }

    @Test
    void aggregateHandlingCommandWithoutIdFieldThrowsAtRegistration() {
        var thrown = assertThrows(org.kendar.cqrses.exceptions.InvalidRegistrationException.class,
                () -> bus.register(AggregateWithIdlessCommand.class));
        assertTrue(thrown.getMessage().contains("NoIdCmd"));
        assertTrue(thrown.getMessage().contains("no annotated field found"));
    }

    @Test
    void aggregateHandlingCommandWithNonUuidIdThrowsAtRegistration() {
        var thrown = assertThrows(org.kendar.cqrses.exceptions.InvalidRegistrationException.class,
                () -> bus.register(AggregateWithStringIdCommand.class));
        assertTrue(thrown.getMessage().contains("StringIdCmd"));
        assertTrue(thrown.getMessage().contains("java.lang.String"));
    }

    @Test
    void interceptorCommandsStayExemptFromIdValidation() {
        bus.register(InterceptorHandlingIdlessCommand.class);
        assertTrue(bus.subscribedClasses.contains(InterceptorHandlingIdlessCommand.class));
    }

    @Command
    public static class NoIdCmd {
        public String name;
    }

    @Command
    public static class StringIdCmd {
        @AggregateIdentifier
        public String id;
    }

    @Command
    public static class GoodCmd {
        @AggregateIdentifier
        public UUID id;
    }

    @Aggregate(group = "agg-good")
    public static class AggregateWithGoodCommand {
        @org.kendar.cqrses.annotations.CommandHandler
        public void handle(GoodCmd cmd) {
        }
    }

    @Aggregate(group = "agg-noid")
    public static class AggregateWithIdlessCommand {
        @org.kendar.cqrses.annotations.CommandHandler
        public void handle(NoIdCmd cmd) {
        }
    }

    @Aggregate(group = "agg-strid")
    public static class AggregateWithStringIdCommand {
        @org.kendar.cqrses.annotations.CommandHandler
        public void handle(StringIdCmd cmd) {
        }
    }

    @CommandInterceptor(group = "int-noid")
    public static class InterceptorHandlingIdlessCommand {
        public void intercept(NoIdCmd cmd) {
        }
    }

    // --- CommandForwarding hook --------------------------------------------------

    @Test
    void sendSyncReturnsForwardedResultWithoutLocalDispatch() {
        var forwarder = new StubForwarder();
        forwarder.syncResult = Optional.of(new CommandForwarder.ForwardResult("remote-value"));
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        String result = bus.sendSync(cmd);

        assertEquals("remote-value", result);
        assertTrue(bus.sendSyncContexts.isEmpty());
        assertEquals(1, forwarder.syncCalls.size());
        assertEquals(cmd.id, forwarder.syncCalls.get(0).getAggregateId());
    }

    @Test
    void sendSyncForwardedNullResultStaysNullAndSkipsLocal() {
        var forwarder = new StubForwarder();
        forwarder.syncResult = Optional.of(new CommandForwarder.ForwardResult(null));
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        assertNull(bus.sendSync(cmd));
        assertTrue(bus.sendSyncContexts.isEmpty());
    }

    @Test
    void sendSyncFallsBackToLocalWhenForwarderDeclines() {
        var forwarder = new StubForwarder();
        forwarder.syncResult = Optional.empty();
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.sendSync(cmd);

        assertEquals(1, forwarder.syncCalls.size());
        assertEquals(1, bus.sendSyncContexts.size());
    }

    @Test
    void sendSyncPropagatesRemoteException() {
        var forwarder = new StubForwarder();
        forwarder.syncError = new RemoteCommandException("x.y.Boom", "kaboom", "node-2");
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        var thrown = assertThrows(RemoteCommandException.class, () -> bus.sendSync(cmd));
        assertEquals("x.y.Boom", thrown.getRemoteExceptionClass());
        assertTrue(bus.sendSyncContexts.isEmpty());
    }

    @Test
    void sendSyncLocalBypassesInstalledForwarder() {
        var forwarder = new StubForwarder();
        forwarder.syncResult = Optional.of(new CommandForwarder.ForwardResult("remote-value"));
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.sendSyncLocal(cmd, 7);

        assertTrue(forwarder.syncCalls.isEmpty());
        assertEquals(1, bus.sendSyncContexts.size());
        assertEquals(7L, bus.sendSyncContexts.get(0).getAggregateVersion());
    }

    @Test
    void sendSkipsLocalWhenForwarderAcks() {
        var forwarder = new StubForwarder();
        forwarder.asyncAcked = true;
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd);

        assertEquals(1, forwarder.asyncCalls.size());
        assertTrue(bus.sendContexts.isEmpty());
    }

    @Test
    void sendFallsBackToLocalWhenForwarderDeclines() {
        var forwarder = new StubForwarder();
        forwarder.asyncAcked = false;
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.send(cmd);

        assertEquals(1, forwarder.asyncCalls.size());
        assertEquals(1, bus.sendContexts.size());
    }

    @Test
    void sendLocalBypassesInstalledForwarder() {
        var forwarder = new StubForwarder();
        forwarder.asyncAcked = true;
        CommandForwarding.install(forwarder);

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.sendLocal(cmd);

        assertTrue(forwarder.asyncCalls.isEmpty());
        assertEquals(1, bus.sendContexts.size());
    }

    @Test
    void resetRestoresPlainLocalDispatch() {
        var forwarder = new StubForwarder();
        forwarder.syncResult = Optional.of(new CommandForwarder.ForwardResult("remote-value"));
        CommandForwarding.install(forwarder);
        CommandForwarding.reset();

        VersionedCmd cmd = new VersionedCmd();
        cmd.id = UUIDGenerator.newUuid();
        bus.sendSync(cmd);

        assertTrue(forwarder.syncCalls.isEmpty());
        assertEquals(1, bus.sendSyncContexts.size());
    }

    public static class StubForwarder implements CommandForwarder {
        public final List<Context> syncCalls = new ArrayList<>();
        public final List<Context> asyncCalls = new ArrayList<>();
        public Optional<ForwardResult> syncResult = Optional.empty();
        public RuntimeException syncError;
        public boolean asyncAcked;

        @Override
        public Optional<ForwardResult> forwardSync(Object command, Context context) {
            syncCalls.add(context);
            if (syncError != null) throw syncError;
            return syncResult;
        }

        @Override
        public boolean forwardAsync(Object command, Context context) {
            asyncCalls.add(context);
            return asyncAcked;
        }
    }

    @Command(version = 3)
    public static class VersionedCmd {
        @AggregateIdentifier
        public UUID id;
    }

    @Command
    public static class DefaultVersionCmd {
        @AggregateIdentifier
        public UUID id;
    }

    @Aggregate(group = "agg-g")
    public static class AggregateClass {
    }

    // --- registerInternal -------------------------------------------------------

    @CommandInterceptor(group = "interceptor-g")
    public static class InterceptorClass {
    }

    public static class PlainClass {
    }

    @org.kendar.cqrses.annotations.Saga
    public static class SagaSideClass {
    }

    public static class StubEventStore implements EventStore {
        @Override
        public void appendEvents(List<InternalMessage> events) {
        }

        @Override
        public List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion) {
            return List.of();
        }

        @Override
        public Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId) {
            return Optional.empty();
        }

        @Override
        public void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion) {
        }

        @Override
        public <T> Optional<T> loadAggregate(UUID aggregateId, Class<T> clazz) {
            return Optional.empty();
        }
    }

    public static class CapturingCommandBus extends CommandBus {
        public final List<Context> sendContexts = new ArrayList<>();
        public final List<Context> sendSyncContexts = new ArrayList<>();
        public final List<Object> sendCommands = new ArrayList<>();

        public CapturingCommandBus(MessageSerializer s, EventStore es) {
            super(s, es);
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
        }

        @Override
        void send(Object command, Context context) {
            sendCommands.add(command);
            sendContexts.add(context);
        }

        @Override
        Object sendSync(Object command, Context context) {
            sendSyncContexts.add(context);
            return null;
        }
    }
}
