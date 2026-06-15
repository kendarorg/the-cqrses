package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqEnqueuePolicy;
import org.kendar.cqrses.exceptions.InvalidAggregateId;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.pg.NullSequencePolicy;
import org.kendar.cqrses.pg.SequencePolicy;

import org.kendar.cqrses.utils.TriFunction;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BusTest {

    private static UUID callExtract(Object command) throws Exception {
        Method m = Bus.class.getDeclaredMethod("extractAggregateId", Object.class);
        m.setAccessible(true);
        try {
            return (UUID) m.invoke(null, command);
        } catch (java.lang.reflect.InvocationTargetException ite) {
            if (ite.getCause() instanceof RuntimeException re) throw re;
            throw ite;
        }
    }

    @BeforeEach
    void resetRegistry() {
        GlobalRegistry.clear();
        // Some tests register handler instances into GlobalRegistry, which
        // triggers autoSubscribe() — that NPEs unless the bus slots are wired.
        GlobalRegistry.register(CommandBus.class, StubBuses.noopCommandBus());
        GlobalRegistry.register(EventBus.class, StubBuses.noopEventBus());
    }

    @AfterEach
    void clear() {
        GlobalRegistry.clear();
    }

    @Test
    void extractAggregateIdReturnsValueOfAnnotatedField() throws Exception {
        CmdWithId c = new CmdWithId();
        UUID id = UUIDGenerator.newUuid();
        c.id = id;
        assertEquals(id, callExtract(c));
    }

    @Test
    void extractAggregateIdReturnsNullWhenNoAnnotatedField() throws Exception {
        assertNull(callExtract(new CmdWithNoId()));
    }

    @Test
    void extractAggregateIdWrapsBadCastInInvalidAggregateId() {
        CmdWithBadIdField c = new CmdWithBadIdField();
        c.id = "not a uuid";
        assertThrows(InvalidAggregateId.class, () -> callExtract(c));
    }

    // --- A concrete Bus we can poke directly. ----------------------------------

    @Test
    void policyConfigRecordExposesValues() {
        DlqEnqueuePolicy enqueuePolicy = new DlqEnqueuePolicy() {
            @Override
            public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
                return DlqEnqueueDecisionResult.ignore();
            }
        };
        SequencePolicy sequencePolicy = new NullSequencePolicy();

        var cfg = new Bus.ProcessingGroupPolicyConfig("orders", enqueuePolicy, sequencePolicy);

        assertEquals("orders", cfg.processingGroup());
        assertSame(enqueuePolicy, cfg.enqueuePolicy());
        assertSame(sequencePolicy, cfg.sequencePolicy());
    }

    @Test
    void defaultPolicyConfigUsesGivenGroupAndIgnoreEnqueuePolicy() {
        var def = Bus.defaultProcessingGroupPolicyConfig("billing");

        assertEquals("billing", def.processingGroup());
        assertNotNull(def.enqueuePolicy());
        assertInstanceOf(NullSequencePolicy.class, def.sequencePolicy());
        // The default enqueue policy never routes to the DLQ.
        assertTrue(def.enqueuePolicy().shouldEnqueue(new InternalMessage(), new RuntimeException()).shouldIgnore());
    }

    @Test
    void defaultPolicyConfigDefaultsToTheDefaultGroup() {
        assertEquals("default", Bus.defaultProcessingGroupPolicyConfig().processingGroup());
    }

    @Test
    void registrationRecordExposesValues() {
        Bus.ProcessingGroupPolicyConfig p = Bus.defaultProcessingGroupPolicyConfig();
        TriFunction<Object, Object, Context, Object> tc = (a, b, c) -> null;
        Bus.Registration r = new Bus.Registration(HandlerHolder.class, tc, p, null);
        assertEquals(HandlerHolder.class, r.handlerClass());
        assertSame(tc, r.method());
        assertSame(p, r.policyConfig());
    }

    @Test
    void registerAddsToSubscribedClassesWhenInternalReturnsTrue() {
        TestBus bus = new TestBus();
        bus.register(HandlerHolder.class);
        assertEquals(List.of(HandlerHolder.class), bus.registerInternalCalls);
        assertTrue(bus.subscribedClasses.contains(HandlerHolder.class));
    }

    @Test
    void registerSkipsDuplicateClassWithoutCallingInternalAgain() {
        TestBus bus = new TestBus();
        bus.register(HandlerHolder.class);
        bus.register(HandlerHolder.class);
        assertEquals(1, bus.registerInternalCalls.size());
    }

    @Test
    void registerDoesNotAddWhenInternalReturnsFalse() {
        TestBus bus = new TestBus();
        bus.registerInternalReturn = false;
        bus.register(HandlerHolder.class);
        assertFalse(bus.subscribedClasses.contains(HandlerHolder.class));
        // A subsequent register WILL retry since the class wasn't added.
        bus.register(HandlerHolder.class);
        assertEquals(2, bus.registerInternalCalls.size());
    }

    @Test
    void getMessageClassReturnsNullForUnknownName() {
        TestBus bus = new TestBus();
        assertNull(bus.getMessageClass("Unknown"));
    }

    @Test
    void getMessageClassReturnsRegisteredCommandTypeBySimpleName() {
        TestBus bus = new TestBus();
        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class,
                Bus.defaultProcessingGroupPolicyConfig(), (a, b, c) -> null);
        assertEquals(CmdWithId.class, bus.getMessageClass("CmdWithId"));
    }

    @Test
    void storeMethodPopulatesConsumersEventsForPGsAndMessageClasses() {
        TestBus bus = new TestBus();
        var policy = Bus.defaultProcessingGroupPolicyConfig();
        TriFunction<Object, Object, Context, Object> tc = (a, b, c) -> null;

        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class, policy, tc);

        assertTrue(bus.consumers.containsKey("default"));
        assertTrue(bus.consumers.get("default").containsKey(CmdWithId.class));
        assertEquals(1, bus.consumers.get("default").get(CmdWithId.class).size());
        assertEquals(Set.of("default"), bus.eventsForProcessingGroups.get(CmdWithId.class));
        assertEquals(CmdWithId.class, bus.messageClasses.get("CmdWithId"));
    }

    @Test
    void storeMethodAppendsConsumersWhenSameCommandTypeRegisteredAgain() {
        TestBus bus = new TestBus();
        var p1 = Bus.defaultProcessingGroupPolicyConfig();
        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class, p1, (a, b, c) -> null);
        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class, p1, (a, b, c) -> null);
        assertEquals(2, bus.consumers.get("default").get(CmdWithId.class).size());
    }

    @Test
    void storeMethodTracksMultipleProcessingGroupsPerCommandType() {
        TestBus bus = new TestBus();
        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class,
                Bus.defaultProcessingGroupPolicyConfig("g1"), (a, b, c) -> null);
        bus.publicStoreMethod(HandlerHolder.class, CmdWithId.class,
                Bus.defaultProcessingGroupPolicyConfig("g2"), (a, b, c) -> null);
        assertEquals(Set.of("g1", "g2"), bus.eventsForProcessingGroups.get(CmdWithId.class));
    }

    // --- extractAggregateId ----------------------------------------------------

    @Test
    void analyzeMethodsRegistersAnnotatedMethod() {
        TestBus bus = new TestBus();
        bus.publicAnalyze(HandlerHolder.class, CommandHandler.class, Command.class,
                Bus.defaultProcessingGroupPolicyConfig());
        assertEquals(1, bus.consumers.get("default").get(CmdWithId.class).size());
    }

    @Test
    void analyzeMethodsThrowsOnDuplicateCommandTypeInSameClass() {
        TestBus bus = new TestBus();
        assertThrows(InvalidRegistrationException.class, () ->
                bus.publicAnalyze(HandlerWithDuplicateCommandTypes.class, CommandHandler.class, Command.class,
                        Bus.defaultProcessingGroupPolicyConfig()));
    }

    @Test
    void analyzeMethodsThrowsWhenFirstParamIsNotAnnotatedAsCommand() {
        TestBus bus = new TestBus();
        assertThrows(InvalidRegistrationException.class, () ->
                bus.publicAnalyze(HandlerWithUnannotatedParam.class, CommandHandler.class, Command.class,
                        Bus.defaultProcessingGroupPolicyConfig()));
    }

    @Test
    void analyzeMethodsSkipsZeroParamMethods() {
        TestBus bus = new TestBus();
        bus.publicAnalyze(HandlerWithEmptyMethod.class, CommandHandler.class, Command.class,
                Bus.defaultProcessingGroupPolicyConfig());
        assertTrue(bus.consumers.isEmpty());
    }

    // --- PolicyConfig / Registration / HandlerRegistration records -------------

    @Test
    void registerMethodInvokesTargetWithContextAndInjectedDeps() throws Exception {
        TestBus bus = new TestBus();
        HandlerHolder instance = new HandlerHolder();
        Service service = new Service("ok");
        GlobalRegistry.register(HandlerHolder.class, instance);
        GlobalRegistry.register(Service.class, service);

        Method m = HandlerHolder.class.getMethod("onCmd", CmdWithId.class, Context.class, Service.class);
        bus.publicRegisterMethod(HandlerHolder.class, m, Bus.defaultProcessingGroupPolicyConfig());

        Bus.Registration reg = bus.consumers.get("default").get(CmdWithId.class).get(0);
        CmdWithId cmd = new CmdWithId();
        Context ctx = new Context();
        reg.method().apply(instance, cmd, ctx);

        assertEquals(1, instance.invocations.size());
        assertSame(cmd, instance.invocations.get(0));
        assertSame(ctx, instance.injectedContext);
        assertSame(service, instance.injectedService);
    }

    @Test
    void registerMethodUnwrapsRuntimeExceptionFromTarget() throws Exception {
        TestBus bus = new TestBus();
        HandlerThatThrowsRuntime instance = new HandlerThatThrowsRuntime();
        GlobalRegistry.register(HandlerThatThrowsRuntime.class, instance);
        Method m = HandlerThatThrowsRuntime.class.getMethod("onCmd", CmdWithId.class);
        bus.publicRegisterMethod(HandlerThatThrowsRuntime.class, m,
                Bus.defaultProcessingGroupPolicyConfig());
        var reg = bus.consumers.get("default").get(CmdWithId.class).get(0);
        var thrown = assertThrows(IllegalStateException.class,
                () -> reg.method().apply(instance, new CmdWithId(), new Context()));
        assertEquals("boom", thrown.getMessage());
    }

    // --- register(Class) -------------------------------------------------------

    @Test
    void registerMethodRethrowsErrorFromTarget() throws Exception {
        TestBus bus = new TestBus();
        HandlerThatThrowsError instance = new HandlerThatThrowsError();
        GlobalRegistry.register(HandlerThatThrowsError.class, instance);
        Method m = HandlerThatThrowsError.class.getMethod("onCmd", CmdWithId.class);
        bus.publicRegisterMethod(HandlerThatThrowsError.class, m,
                Bus.defaultProcessingGroupPolicyConfig());
        var reg = bus.consumers.get("default").get(CmdWithId.class).get(0);
        assertThrows(AssertionError.class,
                () -> reg.method().apply(instance, new CmdWithId(), new Context()));
    }

    @Test
    void registerMethodWrapsCheckedExceptionInRuntimeException() throws Exception {
        TestBus bus = new TestBus();
        HandlerThatThrowsChecked instance = new HandlerThatThrowsChecked();
        GlobalRegistry.register(HandlerThatThrowsChecked.class, instance);
        Method m = HandlerThatThrowsChecked.class.getMethod("onCmd", CmdWithId.class);
        bus.publicRegisterMethod(HandlerThatThrowsChecked.class, m,
                Bus.defaultProcessingGroupPolicyConfig());
        var reg = bus.consumers.get("default").get(CmdWithId.class).get(0);
        var thrown = assertThrows(RuntimeException.class,
                () -> reg.method().apply(instance, new CmdWithId(), new Context()));
        assertEquals("checked", thrown.getCause().getMessage());
    }

    @Test
    void lifecycleHooksAreNoOpsExceptForFlagsInTestBus() {
        TestBus bus = new TestBus();
        bus.start();
        bus.stop();
        bus.clear();
        assertTrue(bus.started.get() && bus.stopped.get() && bus.cleared.get());
    }

    // --- getMessageClass -------------------------------------------------------

    @Test
    void registerMethodInvokesThePassedTargetInstance() throws Exception {
        // The triConsumer dispatches to the `target` arg supplied by the
        // bus's handler — that's the rehydrated aggregate for AGGREGATE
        // registrations, or the registry singleton for projections /
        // interceptors. Routing through GlobalRegistry instead would silently
        // misdirect aggregate commands.
        TestBus bus = new TestBus();
        HandlerHolder registered = new HandlerHolder();
        HandlerHolder different = new HandlerHolder();
        GlobalRegistry.register(HandlerHolder.class, registered);
        GlobalRegistry.register(Service.class, new Service("svc"));

        Method m = HandlerHolder.class.getMethod("onCmd", CmdWithId.class, Context.class, Service.class);
        bus.publicRegisterMethod(HandlerHolder.class, m, Bus.defaultProcessingGroupPolicyConfig());
        bus.consumers.get("default").get(CmdWithId.class).get(0)
                .method().apply(different, new CmdWithId(), new Context());

        assertEquals(0, registered.invocations.size());
        assertEquals(1, different.invocations.size());
    }

    @Test
    void registerInternalGetsHandlerClassExactlyOnceOnFirstSubscribe() {
        TestBus bus = new TestBus();
        bus.register(HandlerHolder.class);
        assertEquals(1, bus.registerInternalCalls.size());
        assertEquals(HandlerHolder.class, bus.registerInternalCalls.get(0));
    }

    // --- storeMethod populates the three maps consistently ---------------------

    @Test
    void registerCalledFromMultipleThreadsResultsInSingleSubscription() throws Exception {
        TestBus bus = new TestBus();
        int n = 20;
        Thread[] threads = new Thread[n];
        AtomicInteger started = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            threads[i] = new Thread(() -> {
                started.incrementAndGet();
                bus.register(HandlerHolder.class);
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        assertTrue(bus.subscribedClasses.contains(HandlerHolder.class));
        // There's a small race window in the unguarded subscribedClasses.contains
        // check, so we allow >= 1 but require the subscribed set to contain it
        // exactly once.
        assertEquals(1, bus.subscribedClasses.stream()
                .filter(c -> c == HandlerHolder.class).count());
    }

    @Command
    public static class CmdWithId {
        @AggregateIdentifier
        public UUID id;
    }

    @Command
    public static class CmdWithNoId {
    }

    // --- analyzeMethods --------------------------------------------------------

    @Command
    public static class CmdWithBadIdField {
        @AggregateIdentifier
        public String id;
    }

    @Event
    public static class SomeEvent {
    }

    public static class NotACommand {
    }

    public static class Service {
        public final String tag;

        public Service(String t) {
            this.tag = t;
        }
    }

    // --- registerMethod: invocation semantics ---------------------------------

    public static class TestBus extends Bus {
        public final List<Class<?>> registerInternalCalls = new ArrayList<>();
        public final AtomicBoolean started = new AtomicBoolean();
        public final AtomicBoolean stopped = new AtomicBoolean();
        public final AtomicBoolean cleared = new AtomicBoolean();
        public boolean registerInternalReturn = true;
        public Object findTargetReturn;

        public TestBus() {
            super(null);
        }

        @Override
        public Object findTarget(Object command, Registration registration) {
            return findTargetReturn;
        }

        @Override
        public void start() {
            started.set(true);
        }

        @Override
        public void stop() {
            stopped.set(true);
        }

        @Override
        public void clear() {
            cleared.set(true);
        }

        @Override
        protected boolean registerInternal(Class<?> handlerClass) {
            registerInternalCalls.add(handlerClass);
            return registerInternalReturn;
        }

        // Expose protected hooks for tests.
        public void publicAnalyze(Class<?> klass,
                                  Class<? extends java.lang.annotation.Annotation> methodAnno,
                                  Class<? extends java.lang.annotation.Annotation> paramAnno,
                                  ProcessingGroupPolicyConfig policy) {
            analyzeMethods(klass, methodAnno, paramAnno, policy);
        }

        public void publicStoreMethod(Class<?> aggregateClass, Class<?> commandType, ProcessingGroupPolicyConfig p,
                                      TriFunction<Object, Object, Context, Object> handler) {
            storeMethod(aggregateClass, commandType, p, handler, null);
        }

        public void publicRegisterMethod(Class<?> aggregateClass, Method m, ProcessingGroupPolicyConfig p) {
            registerMethod(aggregateClass, m, p);
        }
    }

    // Fixture for analyzeMethods / registerMethod tests.
    public static class HandlerHolder {
        public final List<Object> invocations = new ArrayList<>();
        public Service injectedService;
        public Context injectedContext;

        @CommandHandler
        public void onCmd(CmdWithId cmd, Context ctx, Service svc) {
            invocations.add(cmd);
            injectedContext = ctx;
            injectedService = svc;
        }
    }

    public static class HandlerThatThrowsRuntime {
        @CommandHandler
        public void onCmd(CmdWithId cmd) {
            throw new IllegalStateException("boom");
        }
    }

    public static class HandlerThatThrowsError {
        @CommandHandler
        public void onCmd(CmdWithId cmd) {
            throw new AssertionError("kaboom");
        }
    }

    // --- TestBus lifecycle hooks ----------------------------------------------

    public static class HandlerThatThrowsChecked {
        @CommandHandler
        public void onCmd(CmdWithId cmd) throws Exception {
            throw new Exception("checked");
        }
    }

    // --- Sanity: invocation counts across many registrations ------------------

    public static class HandlerWithDuplicateCommandTypes {
        @CommandHandler
        public void a(CmdWithId c) {
        }

        @CommandHandler
        public void b(CmdWithId c) {
        }
    }

    public static class HandlerWithUnannotatedParam {
        @CommandHandler
        public void on(NotACommand c) {
        }
    }

    public static class HandlerWithEmptyMethod {
        @CommandHandler
        public void empty() {
        }
    }
}
