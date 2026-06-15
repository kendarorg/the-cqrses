package org.kendar.cqrses.di;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.utils.TriConsumer;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class GlobalRegistryTest {

    @AfterEach
    void cleanup() {
        GlobalRegistry.clear();
        GlobalRegistry.setFallbackResolver(null);
    }

    @BeforeEach
    void resetState() {
        GlobalRegistry.clear();
        GlobalRegistry.setFallbackResolver(null);
        // autoSubscribe() dereferences the bus slots in the registry; without
        // them, registering anything command- or event-side NPEs.
        GlobalRegistry.register(org.kendar.cqrses.bus.CommandBus.class,
                org.kendar.cqrses.bus.StubBuses.noopCommandBus());
        GlobalRegistry.register(org.kendar.cqrses.bus.EventBus.class,
                org.kendar.cqrses.bus.StubBuses.noopEventBus());
    }

    @Test
    void registerInstanceAndGetReturnsSameObject() {
        Service s = new Service();
        GlobalRegistry.register(Service.class, s);
        assertSame(s, GlobalRegistry.get(Service.class));
    }

    @Test
    void getReturnsNullWhenNothingRegisteredAndNoFallback() {
        assertNull(GlobalRegistry.get(Service.class));
    }

    @Test
    void findReturnsOptionalEmptyWhenAbsent() {
        Optional<Service> opt = GlobalRegistry.find(Service.class);
        assertTrue(opt.isEmpty());
    }

    @Test
    void findReturnsOptionalPresentWhenRegistered() {
        Service s = new Service();
        GlobalRegistry.register(Service.class, s);
        assertEquals(Optional.of(s), GlobalRegistry.find(Service.class));
    }

    @Test
    void fallbackResolverProducesValueOnMissAndCachesIt() {
        AtomicReference<Integer> calls = new AtomicReference<>(0);
        Service produced = new Service();
        GlobalRegistry.setFallbackResolver(t -> {
            calls.updateAndGet(v -> v + 1);
            return t == Service.class ? produced : null;
        });

        Service first = GlobalRegistry.get(Service.class);
        Service second = GlobalRegistry.get(Service.class);
        assertSame(produced, first);
        assertSame(first, second);
        // Cached after the first resolution → resolver only called once.
        assertEquals(1, calls.get());
    }

    @Test
    void fallbackResolverReturningNullDoesNotCacheAnything() {
        GlobalRegistry.setFallbackResolver(t -> null);
        int sizeBefore = GlobalRegistry.allInstances().size();
        assertNull(GlobalRegistry.get(Service.class));
        assertEquals(sizeBefore, GlobalRegistry.allInstances().size());
    }

    @Test
    void clearRemovesEverything() {
        GlobalRegistry.register(Service.class, new Service());
        GlobalRegistry.clear();
        assertNull(GlobalRegistry.get(Service.class));
        assertTrue(GlobalRegistry.allInstances().isEmpty());
    }

    @Test
    void registerMultiAndGetMultiReturnsAllInstancesInOrder() {
        Service a = new Service();
        Service b = new Service();
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(a, b));
        assertEquals(java.util.List.of(a, b), GlobalRegistry.getMulti(Service.class));
    }

    @Test
    void getMultiReturnsEmptyListWhenNothingRegistered() {
        assertTrue(GlobalRegistry.getMulti(Service.class).isEmpty());
    }

    @Test
    void registerMultiAppendsToExistingList() {
        Service a = new Service();
        Service b = new Service();
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(a));
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(b));
        assertEquals(java.util.List.of(a, b), GlobalRegistry.getMulti(Service.class));
    }

    @Test
    void getMultiReturnsImmutableList() {
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(new Service()));
        assertThrows(UnsupportedOperationException.class,
                () -> GlobalRegistry.getMulti(Service.class).clear());
    }

    @Test
    void registerMultiSnapshotsSoLaterMutationOfSourceListDoesNotLeak() {
        var source = new java.util.ArrayList<Object>();
        Service a = new Service();
        source.add(a);
        GlobalRegistry.registerMulti(Service.class, source);
        source.add(new Service());
        assertEquals(java.util.List.of(a), GlobalRegistry.getMulti(Service.class));
    }

    @Test
    void clearRemovesMultiRegistrations() {
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(new Service()));
        GlobalRegistry.clear();
        assertTrue(GlobalRegistry.getMulti(Service.class).isEmpty());
    }

    @Test
    void multiRegistryIsSeparateFromSingleRegistry() {
        Service single = new Service();
        Service multi = new Service();
        GlobalRegistry.register(Service.class, single);
        GlobalRegistry.registerMulti(Service.class, java.util.List.of(multi));
        // The two binding spaces don't interfere with each other.
        assertSame(single, GlobalRegistry.get(Service.class));
        assertEquals(java.util.List.of(multi), GlobalRegistry.getMulti(Service.class));
    }

    @Test
    void allInstancesIsImmutableView() {
        GlobalRegistry.register(Service.class, new Service());
        assertThrows(UnsupportedOperationException.class,
                () -> GlobalRegistry.allInstances().clear());
    }

    @Test
    void registerClassAssignsTargetTypeAggregate() {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        assertEquals(TargetType.AGGREGATE,
                GlobalRegistry.getTargetType(AggregateWithSingleEventHandler.class));
    }

    @Test
    void registerClassAssignsTargetTypeNoneForPlainClass() {
        GlobalRegistry.register(Service.class);
        assertEquals(TargetType.NONE, GlobalRegistry.getTargetType(Service.class));
    }

    @Test
    void getTargetTypeReturnsNullForUnknownType() {
        assertNull(GlobalRegistry.getTargetType(Service.class));
    }

    @Test
    void registeringAggregateRegistersItsEventHandlers() {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        TriConsumer<Object, Object, Context> applier = GlobalRegistry.getEventHandler(
                AggregateWithSingleEventHandler.class, SampleEvent.class);
        assertNotNull(applier);
    }

    @Test
    void getEventHandlerOverloadByEventInstance() {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        TriConsumer<Object, Object, Context> applier = GlobalRegistry.getEventHandler(
                AggregateWithSingleEventHandler.class, new SampleEvent());
        assertNotNull(applier);
    }

    @Test
    void getEventHandlerReturnsNullForUnknownAggregate() {
        assertNull(GlobalRegistry.getEventHandler(Service.class, SampleEvent.class));
    }

    @Test
    void getEventHandlerReturnsNullForUnregisteredEventType() {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        assertNull(GlobalRegistry.getEventHandler(
                AggregateWithSingleEventHandler.class, OtherEvent.class));
    }

    @Test
    void aggregateApplierInvokesAnnotatedMethod() throws Exception {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        TriConsumer<Object, Object, Context> applier =
                GlobalRegistry.getEventHandler(AggregateWithSingleEventHandler.class, SampleEvent.class);
        AggregateWithSingleEventHandler inst = new AggregateWithSingleEventHandler();
        // Should invoke without throwing
        applier.accept(inst, new SampleEvent(), new Context());
    }

    @Test
    void duplicateEventHandlerThrowsInvalidRegistrationException() {
        assertThrows(InvalidRegistrationException.class,
                () -> GlobalRegistry.register(AggregateWithDuplicateEventHandlers.class));
    }

    @Test
    void unannotatedEventParamThrowsInvalidRegistrationException() {
        assertThrows(InvalidRegistrationException.class,
                () -> GlobalRegistry.register(AggregateWithUnannotatedEventParam.class));
    }

    @Test
    void getFieldAnnotatedWithReturnsTheAnnotatedField() {
        Field f = GlobalRegistry.getFieldAnnotatedWith(CommandHolder.class, AggregateIdentifier.class);
        assertNotNull(f);
        assertEquals("anId", f.getName());
        assertTrue(f.canAccess(new CommandHolder()));
    }

    @Test
    void getFieldAnnotatedWithReturnsNullWhenAbsent() {
        assertNull(GlobalRegistry.getFieldAnnotatedWith(Service.class, AggregateIdentifier.class));
    }

    @Test
    void applierInjectsContextAndResolvesOtherParamsFromRegistry() {
        Service svc = new Service();
        GlobalRegistry.register(Service.class, svc);
        GlobalRegistry.register(AggregateWithDeps.class);

        var applier = GlobalRegistry.getEventHandler(AggregateWithDeps.class, SampleEvent.class);
        assertNotNull(applier);

        AggregateWithDeps inst = new AggregateWithDeps();
        SampleEvent evt = new SampleEvent();
        Context ctx = new Context();
        applier.accept(inst, evt, ctx);

        assertSame(evt, inst.received);
        assertSame(ctx, inst.receivedContext);
        assertSame(svc, inst.receivedService);
    }

    @Test
    void applierInvokesTheInstancePassedInNotTheRegistryEntry() {
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
        var applier = GlobalRegistry.getEventHandler(
                AggregateWithSingleEventHandler.class, SampleEvent.class);
        AggregateWithSingleEventHandler inst = new AggregateWithSingleEventHandler();
        AggregateWithSingleEventHandler other = new AggregateWithSingleEventHandler();
        GlobalRegistry.register(AggregateWithSingleEventHandler.class, other);

        // No exception → applier dispatches to the passed-in `inst`, not `other`.
        applier.accept(inst, new SampleEvent(), new Context());
    }

    @Test
    void applierUnwrapsRuntimeExceptionFromHandler() {
        GlobalRegistry.register(AggregateThrowingRuntime.class);
        var applier = GlobalRegistry.getEventHandler(AggregateThrowingRuntime.class, SampleEvent.class);
        var thrown = assertThrows(IllegalStateException.class,
                () -> applier.accept(new AggregateThrowingRuntime(), new SampleEvent(), new Context()));
        assertEquals("boom", thrown.getMessage());
    }

    @Test
    void applierRethrowsErrorFromHandler() {
        GlobalRegistry.register(AggregateThrowingError.class);
        var applier = GlobalRegistry.getEventHandler(AggregateThrowingError.class, SampleEvent.class);
        assertThrows(AssertionError.class,
                () -> applier.accept(new AggregateThrowingError(), new SampleEvent(), new Context()));
    }

    @Test
    void applierWrapsCheckedExceptionInRuntimeException() {
        GlobalRegistry.register(AggregateThrowingChecked.class);
        var applier = GlobalRegistry.getEventHandler(AggregateThrowingChecked.class, SampleEvent.class);
        var thrown = assertThrows(RuntimeException.class,
                () -> applier.accept(new AggregateThrowingChecked(), new SampleEvent(), new Context()));
        assertEquals("checked", thrown.getCause().getMessage());
    }

    @Test
    void zeroParamEventHandlerIsSkippedDuringRegistration() {
        // Registering must not throw, and no applier should be wired (no event type to key by).
        assertDoesNotThrow(() -> GlobalRegistry.register(AggregateWithZeroParamHandler.class));
        assertNull(GlobalRegistry.getEventHandler(AggregateWithZeroParamHandler.class, SampleEvent.class));
    }

    @Test
    void eventHandlerInheritedFromSuperclassIsRegistered() {
        GlobalRegistry.register(AggregateChild.class);
        var applier = GlobalRegistry.getEventHandler(AggregateChild.class, SampleEvent.class);
        assertNotNull(applier, "child should inherit parent's @EventHandler");
        AggregateChild inst = new AggregateChild();
        applier.accept(inst, new SampleEvent(), new Context());
        assertEquals(java.util.List.of("parent"), inst.hits);
    }

    @Test
    void subclassOverrideWithoutAnnotationShadowsParentHandler() {
        // Parent declares @EventHandler; subclass overrides without
        // reapplying it → ReflectionUtils dedupes by signature and the
        // unannotated override wins, so no handler should be registered.
        GlobalRegistry.register(AggregateChildOverriding.class);
        assertNull(GlobalRegistry.getEventHandler(AggregateChildOverriding.class, SampleEvent.class));
    }

    @Test
    void getFieldAnnotatedWithIsCached() {
        Field first = GlobalRegistry.getFieldAnnotatedWith(CommandHolder.class, AggregateIdentifier.class);
        Field second = GlobalRegistry.getFieldAnnotatedWith(CommandHolder.class, AggregateIdentifier.class);
        assertSame(first, second);
    }

    @Test
    void registerInstanceIsResolvableByImplementedInterface() {
        ServiceImpl impl = new ServiceImpl();
        GlobalRegistry.register(ServiceImpl.class, impl);
        assertSame(impl, GlobalRegistry.get(ServiceImpl.class));
        assertSame(impl, GlobalRegistry.get(ServiceApi.class));
    }

    @Test
    void registerInstanceIsResolvableBySuperclass() {
        ServiceImpl impl = new ServiceImpl();
        GlobalRegistry.register(ServiceImpl.class, impl);
        assertSame(impl, GlobalRegistry.get(ServiceBase.class));
    }

    @Test
    void registerInstanceIsResolvableByTransitiveSuperInterface() {
        ServiceImpl impl = new ServiceImpl();
        GlobalRegistry.register(ServiceImpl.class, impl);
        assertSame(impl, GlobalRegistry.get(ServiceMarker.class));
    }

    @Test
    void explicitInterfaceRegistrationWinsOverDerivedSupertype() {
        ServiceImpl first = new ServiceImpl();
        ServiceImpl second = new ServiceImpl();
        // Register `first` explicitly under the interface...
        GlobalRegistry.register(ServiceApi.class, first);
        // ...then register `second` under its impl class. Its derived supertype
        // registration must NOT clobber the explicit ServiceApi entry.
        GlobalRegistry.register(ServiceImpl.class, second);
        assertSame(first, GlobalRegistry.get(ServiceApi.class));
        assertSame(second, GlobalRegistry.get(ServiceImpl.class));
    }

    @Test
    void allInstancesDedupesSingleInstanceStoredUnderManyKeys() {
        // One instance is stored under ServiceImpl, ServiceBase, ServiceApi,
        // ServiceMarker — but allInstances() must return it exactly once.
        GlobalRegistry.register(ServiceImpl.class, new ServiceImpl());
        long distinctNonBus = GlobalRegistry.allInstances().stream()
                .filter(o -> o instanceof ServiceImpl)
                .count();
        assertEquals(1, distinctNonBus);
    }

    // --- grill item 1: setup→runtime freeze latch -----------------------------

    @Test
    void notStartedBeforeStart() {
        assertFalse(GlobalRegistry.isStarted());
        // No throw while still in the setup phase.
        GlobalRegistry.assertNotStarted("subscribe");
    }

    @Test
    void startLatchesTopologyClosed() {
        GlobalRegistry.start();
        assertTrue(GlobalRegistry.isStarted());
        assertThrows(IllegalStateException.class,
                () -> GlobalRegistry.assertNotStarted("subscribe"));
    }

    @Test
    void subscribingAfterStartFailsFast() {
        GlobalRegistry.start();
        assertThrows(IllegalStateException.class,
                () -> GlobalRegistry.register(AggregateWithSingleEventHandler.class));
    }

    @Test
    void setSegmentsAfterStartFailsFast() {
        GlobalRegistry.start();
        assertThrows(IllegalStateException.class,
                () -> org.kendar.cqrses.pg.SegmentCalculator.setSegments(7));
    }

    @Test
    void clearReopensSetupPhase() {
        GlobalRegistry.start();
        assertTrue(GlobalRegistry.isStarted());
        GlobalRegistry.clear();
        assertFalse(GlobalRegistry.isStarted());
        // And a fresh subscribe is allowed again (resetState re-registers the buses).
        resetState();
        GlobalRegistry.register(AggregateWithSingleEventHandler.class);
    }

    @Test
    void stopDoesNotResetLatch() {
        GlobalRegistry.start();
        GlobalRegistry.stop();
        assertTrue(GlobalRegistry.isStarted(),
                "a pump bounce (stop) must NOT reopen the setup phase; only clear() does");
    }

    // --- grill item 4: duplicate simple-name guard ----------------------------

    @Test
    void duplicateAggregateSimpleNameRejectedCaseInsensitively() {
        GlobalRegistry.register(NameClash1.Dup.class);
        // NameClash2.DUP folds to the same simple name as NameClash1.Dup.
        assertThrows(InvalidRegistrationException.class,
                () -> GlobalRegistry.register(NameClash2.DUP.class));
    }

    @Test
    void sameAggregateRegisteredTwiceIsNotACollision() {
        GlobalRegistry.register(NameClash1.Dup.class);
        // Idempotent re-registration of the SAME class must not trip the guard.
        GlobalRegistry.register(NameClash1.Dup.class);
    }

    static class NameClash1 {
        @Aggregate
        public static class Dup {
            @AggregateIdentifier
            public UUID id;
        }
    }

    static class NameClash2 {
        @Aggregate
        public static class DUP {
            @AggregateIdentifier
            public UUID id;
        }
    }

    public static class Service {
        public String hello() {
            return "hi";
        }
    }

    interface ServiceMarker {
    }

    interface ServiceApi extends ServiceMarker {
    }

    public static abstract class ServiceBase {
    }

    public static class ServiceImpl extends ServiceBase implements ServiceApi {
    }

    @Event
    public static class SampleEvent {
        public String value;
    }

    @Event
    public static class OtherEvent {
    }

    @Aggregate
    public static class AggregateWithSingleEventHandler {
        @AggregateIdentifier
        public UUID id;

        @EventHandler
        public void on(SampleEvent e) {
        }
    }

    @Aggregate
    public static class AggregateWithDuplicateEventHandlers {
        @EventHandler
        public void onA(SampleEvent e) {
        }

        @EventHandler
        public void onB(SampleEvent e, Context c) {
        }
    }

    @Aggregate
    public static class AggregateWithUnannotatedEventParam {
        @EventHandler
        public void on(NotAnEvent e) {
        }

        public static class NotAnEvent {
        }
    }

    // --- registerAggregateEventHandlers: applier semantics --------------------

    @Aggregate
    public static class AggregateWithDeps {
        public SampleEvent received;
        public Context receivedContext;
        public Service receivedService;

        @EventHandler
        public void on(SampleEvent e, Context ctx, Service svc) {
            this.received = e;
            this.receivedContext = ctx;
            this.receivedService = svc;
        }
    }

    @Aggregate
    public static class AggregateWithZeroParamHandler {
        public int called;

        @EventHandler
        public void noParams() {
            called++;
        }
    }

    @Aggregate
    public static class AggregateThrowingRuntime {
        @EventHandler
        public void on(SampleEvent e) {
            throw new IllegalStateException("boom");
        }
    }

    @Aggregate
    public static class AggregateThrowingError {
        @EventHandler
        public void on(SampleEvent e) {
            throw new AssertionError("kaboom");
        }
    }

    @Aggregate
    public static class AggregateThrowingChecked {
        @EventHandler
        public void on(SampleEvent e) throws Exception {
            throw new Exception("checked");
        }
    }

    @Aggregate
    public static class AggregateParent {
        public final java.util.List<String> hits = new java.util.ArrayList<>();

        @EventHandler
        public void parentHandler(SampleEvent e) {
            hits.add("parent");
        }
    }

    // @Aggregate is not @Inherited, so the child must re-declare it for the
    // registry to classify it as AGGREGATE and walk its event handlers.
    @Aggregate
    public static class AggregateChild extends AggregateParent {
        // No annotation on methods — inherits parent's @EventHandler.
    }

    @Aggregate
    public static class AggregateChildOverriding extends AggregateParent {
        @Override
        public void parentHandler(SampleEvent e) {
            hits.add("child");
        }
    }

    public static class CommandHolder {
        @AggregateIdentifier
        public UUID anId;
    }
}
