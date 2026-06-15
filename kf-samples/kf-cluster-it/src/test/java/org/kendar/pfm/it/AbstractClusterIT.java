package org.kendar.pfm.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Brings up the shared cluster substrate for the {@code *IT} scenarios: one MySQL container
 * (alias {@code kfdb}) and three real JVMs of {@code kf-spring-app} (aliases {@code node1..3}) on a
 * single Docker network, in cluster mode against that one MySQL. Every container port is pinned to a
 * fixed host port (so a debugger / curl from the host has predictable addresses), and JDWP is open
 * on each node. Exposes HTTP helpers for the PFM API + the {@code /cluster/*} control API, and JDBC
 * helpers for asserting cluster coordination state directly in MySQL.
 *
 * <p>Each concrete IT class gets its own fresh MySQL + 3 nodes (JUnit runs the static lifecycle once
 * per class). These tests run for minutes — that is expected; the waits are sized to the real
 * {@code ClusterConfig} timings (mirrored below). Docker-gated: skipped when Docker is unavailable.
 *
 * <p><b>Port map</b> (host:container):
 * <pre>
 *           app(8080)  liveness(8070)  jdwp(5005)
 *   node1:  18081      18071           15005
 *   node2:  18082      18072           15006
 *   node3:  18083      18073           15007
 * </pre>
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractClusterIT {


    private static void forceStaticInitializer() {
        var tt = new CommandForwardingIT();
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger("cluster-it");

    protected static final int SEGMENTS = 6;
    protected static final int NODE_COUNT = 3;

    /** Container-internal ports. */
    static final int APP_PORT = 8080;
    static final int LIVENESS_PORT = 8070;
    static final int JDWP_PORT = 5005;

    /** Pinned host ports per node index (0..2). */
    static final int[] APP_HOST_PORTS = {18081, 18082, 18083};
    static final int[] LIVENESS_HOST_PORTS = {18071, 18072, 18073};
    static final int[] JDWP_HOST_PORTS = {15005, 15006, 15007};

    protected static final String[] NODE_IDS = {"node1", "node2", "node3"};

    /** 1-in-N sampling for the per-command perf traces harvested into the report. */
    protected static final int TRACE_SAMPLE_EVERY = 100;

    // ---- ClusterConfig timings mirrored (ms). Keep in sync with kf-cluster ClusterConfig. ----
    protected static final long HEARTBEAT_MS = 3_000L;
    protected static final long TICK_MS = 5_000L;
    protected static final long LEASE_MS = 30_000L;
    protected static final long MEMBERSHIP_STABILIZE_MS = 10_000L;
    protected static final long LEADER_LOCK_LEASE_MS = 15_000L;
    protected static final long STALENESS_WINDOW_MS = 9_000L;

    /** Internal JDBC URL the app uses to reach MySQL over the Docker network (creds in the URL). */
    private static final String APP_DB_URL =
            "jdbc:mysql://kfdb:3306/kf?user=kf&password=kf&allowPublicKeyRetrieval=true&useSSL=false";

    private static final String DOCKERFILE = """
            FROM eclipse-temurin:25-jre
            COPY app.jar /app/app.jar
            ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]
            """;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    /** Prometheus scrape port (container-internal). */
    static final int PROM_PORT = 9090;

    protected static Network network;
    protected static MySQLContainer<?> mysql;
    protected static final List<GenericContainer<?>> nodes = new ArrayList<>();
    /**
     * Extra env vars applied to every node container. A concrete IT class opts into a
     * feature by populating this in a {@code static {}} initializer (the subclass loads
     * before the base {@code @BeforeAll} runs). Cleared in {@code @AfterAll} so a class
     * cannot leak configuration into the next one on the same forked JVM.
     */
    protected static final Map<String, String> EXTRA_NODE_ENV = new LinkedHashMap<>();
    /** Prometheus scraping all three nodes' /actuator/prometheus over the Docker network. */
    protected static GenericContainer<?> prometheus;

    @BeforeAll
    static void startCluster() {
        forceStaticInitializer();
        network = Network.newNetwork();

        mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.4"))
                .withNetwork(network)
                .withNetworkAliases("kfdb")
                .withDatabaseName("kf")
                .withUsername("kf")
                .withPassword("kf")
                .withUrlParam("allowPublicKeyRetrieval", "true")
                .withUrlParam("useSSL", "false");
        mysql.start();

        ImageFromDockerfile image = buildNodeImage();
        for (int i = 0; i < NODE_COUNT; i++) {
            nodes.add(newNode(i, image));
        }
        // Start node1 first so it creates the (idempotent) schema, then bring up the rest. This
        // avoids three JVMs racing CREATE TABLE on a fresh MySQL.
        for (int i = 0; i < NODE_COUNT; i++) {
            LOGGER.trace("starting {}", NODE_IDS[i]);
            nodes.get(i).start();
        }

        // Prometheus joins the same network and scrapes the three nodes by alias. Started after the
        // nodes so its first scrape lands on live targets; failures here never fail the cluster
        // scenarios (metrics collection is best-effort — guarded in captureMetrics).
        try {
            prometheus = new GenericContainer<>(DockerImageName.parse("prom/prometheus:v2.54.1"))
                    .withNetwork(network)
                    .withNetworkAliases("prometheus")
                    .withExposedPorts(PROM_PORT)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("prometheus.yml"),
                            "/etc/prometheus/prometheus.yml")
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("prometheus")))
                    .waitingFor(Wait.forHttp("/-/ready").forPort(PROM_PORT).forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(60)));
            prometheus.start();
            LOGGER.trace("prometheus up on host port {}", prometheus.getMappedPort(PROM_PORT));
        } catch (RuntimeException e) {
            LOGGER.warn("prometheus failed to start; metric capture disabled for this run: {}", e.getMessage());
            prometheus = null;
        }
    }

    @AfterAll
    static void stopCluster() {
        if (prometheus != null) {
            try {
                prometheus.stop();
            } catch (RuntimeException e) {
                LOGGER.warn("error stopping prometheus: {}", e.getMessage());
            }
            prometheus = null;
        }
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).stop();
            } catch (RuntimeException e) {
                LOGGER.warn("error stopping {}: {}", NODE_IDS[i], e.getMessage());
            }
        }
        nodes.clear();
        EXTRA_NODE_ENV.clear();
        if (mysql != null) {
            try {
                mysql.stop();
            } catch (RuntimeException e) {
                LOGGER.warn("error stopping mysql: {}", e.getMessage());
            }
        }
        if (network != null) {
            try {
                network.close();
            } catch (RuntimeException e) {
                LOGGER.warn("error closing network: {}", e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Test timeline (ms from test start) — recorded for every scenario and written into the md report
    // even when Prometheus never came up, so node start/stop moments and the total run length are
    // always captured. Offsets are relative to {@link #testStartMs} (set in @BeforeEach).
    // ---------------------------------------------------------------------------------------------

    /** One recorded lifecycle moment: offset from test start (ms) + a human label. */
    protected record TimelineEvent(long offsetMs, String label) {}

    private final List<TimelineEvent> timeline = java.util.Collections.synchronizedList(new ArrayList<>());
    /** Wall-clock at @BeforeEach — the zero of every timeline offset. */
    private volatile long testStartMs = 0L;
    /** Report-title suffix for this scenario (test method name, or the flood {@code variant()}). */
    private volatile String scenario = "";

    // ---------------------------------------------------------------------------------------------
    // Client-side command bookkeeping for the run TOTALS block — fed automatically by every
    // login()/recordOp() so any scenario that sends commands gets a TOTALS section (logged at INFO
    // and mirrored into the markdown report) for free. The flood scenarios override the report*()
    // accessors to report their LoadGenerator's own counters instead.
    // ---------------------------------------------------------------------------------------------
    private final java.util.concurrent.atomic.AtomicLong sentOps = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong sentNet = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.Set<String> sentUsers = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Captured TOTALS figures, rendered into the report by {@link #appendReportSections}. */
    private final Map<String, Object> totalsForReport =
            java.util.Collections.synchronizedMap(new LinkedHashMap<>());

    @BeforeEach
    void beginTimeline(TestInfo info) {
        testStartMs = System.currentTimeMillis();
        timeline.clear();
        sentOps.set(0);
        sentNet.set(0);
        sentUsers.clear();
        totalsForReport.clear();
        scenario = scenarioLabel(info);
        event("test-start");
    }

    @AfterEach
    void endTimeline() {
        event("test-end");
        writeAnalysisReport(scenario);
    }

    /** Report-title suffix; the flood base overrides this to use its {@code variant()}. */
    protected String scenarioLabel(TestInfo info) {
        return info.getTestMethod().map(java.lang.reflect.Method::getName)
                .orElse(getClass().getSimpleName());
    }

    /** Record {@code label} at the current offset from test start, and log it at TRACE. */
    protected void event(String label) {
        long off = testStartMs == 0L ? 0L : System.currentTimeMillis() - testStartMs;
        timeline.add(new TimelineEvent(off, label));
        LOGGER.trace("timeline t+{}ms: {}", off, label);
    }

    private static ImageFromDockerfile buildNodeImage() {
        Path bootJar = locateBootJar();
        LOGGER.trace("building node image from boot jar {}", bootJar);
        return new ImageFromDockerfile("kf-pfm-node:it", false)
                .withFileFromPath("app.jar", bootJar)
                .withFileFromString("Dockerfile", DOCKERFILE);
    }

    /** Find the kf-spring-app executable boot jar in the sibling module's target dir. */
    private static Path locateBootJar() {
        Path targetDir = Paths.get("..", "kf-spring-app", "target").toAbsolutePath().normalize();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "kf-spring-app-*.jar")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (name.endsWith("-original.jar") || name.contains("sources") || name.contains("javadoc")) {
                    continue;
                }
                return p;
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + targetDir, e);
        }
        throw new IllegalStateException("kf-spring-app boot jar not found in " + targetDir
                + " — run `mvn -pl kf-samples/kf-spring-app -am package` first");
    }

    private static GenericContainer<?> newNode(int i, ImageFromDockerfile image) {
        GenericContainer<?> node = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(NODE_IDS[i])
                .withExposedPorts(APP_PORT, LIVENESS_PORT, JDWP_PORT)
                .withEnv("PFM_DATASOURCE_URL", APP_DB_URL)
                .withEnv("KF_CLUSTER_ENABLED", "true")
                .withEnv("KF_SEGMENTS", String.valueOf(SEGMENTS))
                .withEnv("KF_LIVENESS_PORT", String.valueOf(LIVENESS_PORT))
                .withEnv("KF_CLUSTER_NODE_ID", NODE_IDS[i])
                .withEnv("KF_CLUSTER_HOST", NODE_IDS[i])
                .withEnv("SERVER_PORT", String.valueOf(APP_PORT))
                // Spring Boot auto-enables ANSI colour on its console; the raw escape codes are what
                // garble the live docker stream ("one char then blank lines"). Force it off so both
                // the live stream and the captured kf-cluster-it.log are plain text.
                .withEnv("SPRING_OUTPUT_ANSI_ENABLED", "NEVER")
                .withEnv("KF_CLUSTER_FORWARDING_ENABLED","true")
                // Bottleneck instrumentation: extended meters (append-phase split,
                // inflight/lag gauges, nudge counters) + sampled per-command traces
                // kept in-memory on each node and harvested over /kf/perf-traces.
                // Relaxed binding: env names drop the dashes (sample-every → SAMPLEEVERY).
                .withEnv("KF_OBSERVABILITY_EXTENDEDMETRICS", "true")
                .withEnv("KF_OBSERVABILITY_TRACE_ENABLED", "true")
                .withEnv("KF_OBSERVABILITY_TRACE_SAMPLEEVERY", String.valueOf(TRACE_SAMPLE_EVERY))
                .withEnv("JAVA_OPTS", javaOpts(i))
                .withEnv(EXTRA_NODE_ENV)
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(NODE_IDS[i])))
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forHttp("/cluster/status").forPort(APP_PORT).forStatusCode(200))
                        .withStrategy(Wait.forHttp("/alive").forPort(LIVENESS_PORT).forStatusCode(200))
                        .withStartupTimeout(Duration.ofSeconds(180)));
        node.setPortBindings(List.of(
                APP_HOST_PORTS[i] + ":" + APP_PORT,
                LIVENESS_HOST_PORTS[i] + ":" + LIVENESS_PORT,
                JDWP_HOST_PORTS[i] + ":" + JDWP_PORT));
        node.dependsOn(mysql);
        return node;
    }

    public void fakeOverride(){
        System.out.println("fakeOverride");
    }


    private static String javaOpts(int i) {
        // server=y → IDE attaches in; suspend=n → don't block startup (cluster timers must run).
        // Opt in to suspend on a chosen node with -DDEBUG_SUSPEND=y [-DDEBUG_SUSPEND_NODE=<idx>].
        boolean suspend = "y".equalsIgnoreCase(System.getProperty("DEBUG_SUSPEND", "n"))
                && i == Integer.getInteger("DEBUG_SUSPEND_NODE", 0);
        return "-agentlib:jdwp=transport=dt_socket,server=y,suspend=" + (suspend ? "y" : "n")
                + ",address=*:" + JDWP_PORT;
    }

    // ---------------------------------------------------------------------------------------------
    // HTTP helpers
    // ---------------------------------------------------------------------------------------------

    protected int appHostPort(int nodeIdx) {
        return nodes.get(nodeIdx).getMappedPort(APP_PORT);
    }

    protected String httpBase(int nodeIdx) {
        return "http://localhost:" + appHostPort(nodeIdx);
    }

    /** GET /cluster/status — raw JSON body. */
    protected String clusterStatus(int nodeIdx) {
        return get(httpBase(nodeIdx) + "/cluster/status");
    }

    protected boolean clusterRunning(int nodeIdx) {
        return jsonBool(clusterStatus(nodeIdx), "running");
    }

    protected String clusterStop(int nodeIdx) {
        event("cluster-stop " + NODE_IDS[nodeIdx]);
        String body = post(httpBase(nodeIdx) + "/cluster/stop", null);
        event("cluster-stopped " + NODE_IDS[nodeIdx]);
        return body;
    }

    protected String clusterStart(int nodeIdx) {
        event("cluster-start " + NODE_IDS[nodeIdx]);
        String body = post(httpBase(nodeIdx) + "/cluster/start", null);
        event("cluster-started " + NODE_IDS[nodeIdx]);
        return body;
    }

    /** Hard-stop node {@code idx}'s container (crash), bracketed by timeline events. */
    protected void stopNodeContainer(int idx) {
        event("container-stop " + NODE_IDS[idx]);
        nodes.get(idx).stop();
        event("container-stopped " + NODE_IDS[idx]);
    }

    /** (Re)start node {@code idx}'s container, bracketed by timeline events (blocks on its wait strategy). */
    protected void startNodeContainer(int idx) {
        event("container-start " + NODE_IDS[idx]);
        nodes.get(idx).start();
        event("container-started " + NODE_IDS[idx]);
    }

    /** POST /api/login {username}. */
    protected void login(int nodeIdx, String user) {
        post(httpBase(nodeIdx) + "/api/login", "{\"username\":\"" + user + "\"}");
        sentUsers.add(user);
    }

    /**
     * POST /api/operations {username,type,amount,tag}. Returns the server-minted {@code opId} from the
     * 2xx response — only reached on success, so the caller can record exactly which ops were acked
     * (the inverse set, durable-but-unacked, is the committed-but-ack-lost forensic evidence).
     */
    protected String recordOp(int nodeIdx, String user, String type, long amount, String tag) {
        String body = post(httpBase(nodeIdx) + "/api/operations",
                "{\"username\":\"" + user + "\",\"type\":\"" + type + "\",\"amount\":" + amount
                        + ",\"tag\":\"" + tag + "\"}");
        // Only reached on a 2xx (send() throws otherwise), so this is an acked op.
        sentOps.incrementAndGet();
        sentNet.addAndGet("IN".equalsIgnoreCase(type) ? amount : -amount);
        sentUsers.add(user);
        return jsonStr(body, "opId");
    }

    /** GET /api/summary?username=... → the {@code net} total. */
    protected long summaryNet(int nodeIdx, String user) {
        return jsonLong(get(httpBase(nodeIdx) + "/api/summary?username=" + user), "net");
    }

    protected String get(String url) {
        return send(HttpRequest.newBuilder(URI.create(url)).GET().build());
    }

    protected String post(String url, String jsonBody) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url));
        if (jsonBody == null) {
            b.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            b.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(jsonBody));
        }
        return send(b.build());
    }

    private String send(HttpRequest request) {
        try {
            HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException(request.method() + " " + request.uri()
                        + " → " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new RuntimeException("HTTP " + request.method() + " " + request.uri() + " failed", e);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Prometheus / metrics capture (best-effort — never fails a scenario)
    // ---------------------------------------------------------------------------------------------

    /** The fixed report query set: column label → PromQL with a {@code %d}-second rate window. */
    private static final Map<String, String> METRIC_QUERIES = new LinkedHashMap<>() {{
        put("cmd_rate_per_s", "sum(rate(kf_command_handle_seconds_count[%ds]))");
        put("cmd_p99_ms", "histogram_quantile(0.99, sum(rate(kf_command_handle_seconds_bucket[%ds])) by (le))*1000");
        put("append_p99_ms", "histogram_quantile(0.99, sum(rate(kf_events_append_seconds_bucket[%ds])) by (le))*1000");
        put("dispatch_rate_per_s", "sum(rate(kf_event_dispatch_seconds_count[%ds]))");
        put("tail_read_p99_ms", "histogram_quantile(0.99, sum(rate(kf_segment_tail_read_seconds_bucket[%ds])) by (le))*1000");
        put("sql_p99_ms", "histogram_quantile(0.99, sum(rate(kf_sql_execute_seconds_bucket[%ds])) by (le))*1000");
        put("sql_event_entry_rate_per_s", "sum(rate(kf_sql_execute_seconds_count{category=\"select:event_entry\"}[%ds]))");
        put("dlq_rate_per_s", "sum(rate(kf_dlq_enqueue_total[%ds]))");
        // Raw command count over the window — rates above are derived; an absolute
        // count is locale-proof and sanity-checks the rate columns.
        put("cmd_count", "sum(increase(kf_command_handle_seconds_count[%ds]))");
        // Extended bottleneck metrics (kf.observability.extended-metrics): the
        // append-phase split that explains where the append wall-clock goes.
        put("append_lock_p99_ms", "histogram_quantile(0.99, sum(rate(kf_append_phase_seconds_bucket{phase=\"lock\"}[%ds])) by (le))*1000");
        put("append_commit_p99_ms", "histogram_quantile(0.99, sum(rate(kf_append_phase_seconds_bucket{phase=\"commit\"}[%ds])) by (le))*1000");
        put("conn_acquire_p99_ms", "histogram_quantile(0.99, sum(rate(hikaricp_connections_acquire_seconds_bucket[%ds])) by (le))*1000");
        put("pool_pending_max", "max(hikaricp_connections_pending)");
        put("pump_lag_max", "max(kf_pump_lag)");
        put("nudge_rate_per_s", "sum(rate(kf_pump_nudge_total[%ds]))");
    }};

    private boolean metricsHeaderWritten = false;
    /** Accumulated per-phase snapshots, replayed into the markdown analysis report. */
    private final List<Map<String, Object>> metricSamples = new ArrayList<>();
    /** Wall-clock of the first captured sample — the full-run rate/quantile window. */
    private long metricsRunStartMs = 0L;

    protected String promBase() {
        return "http://localhost:" + prometheus.getMappedPort(PROM_PORT);
    }

    /**
     * Run one instant PromQL query and return the first series' value (or {@code NaN} when Prometheus
     * is absent, the query errors, or there is no data yet). Best-effort: never throws.
     */
    protected double promScalar(String promql) {
        if (prometheus == null) return Double.NaN;
        try {
            String url = promBase() + "/api/v1/query?query="
                    + URLEncoder.encode(promql, StandardCharsets.UTF_8);
            String body = get(url);
            Matcher m = Pattern.compile("\"value\"\\s*:\\s*\\[\\s*[0-9.]+\\s*,\\s*\"([^\"]*)\"\\s*]").matcher(body);
            if (!m.find()) return Double.NaN;
            return Double.parseDouble(m.group(1));
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }

    /**
     * Snapshot the fixed metric set for a finished load {@code phase} (rate window = the phase length,
     * floored at 6s for histogram stability), log a one-liner, and append a row to
     * {@code target/kf-metrics-<TestClass>.csv}. No-op when Prometheus did not start.
     */
    protected void captureMetrics(String phase, long windowMs) {
        if (prometheus == null) return;
        if (metricsRunStartMs == 0L) metricsRunStartMs = now();
        int w = (int) Math.max(6, windowMs / 1000);
        Map<String, Double> values = new LinkedHashMap<>();
        for (var e : METRIC_QUERIES.entrySet()) {
            values.put(e.getKey(), promScalar(String.format(e.getValue(), w)));
        }
        LOGGER.trace("metrics [{}]: {}", phase, values.entrySet().stream()
                .map(e -> e.getKey() + "=" + fmt(e.getValue()))
                .reduce((a, b) -> a + ", " + b).orElse(""));
        event("metrics phase: " + phase);
        // Remember the snapshot for the consolidated markdown report.
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("phase", phase);
        sample.put("ts_ms", now());
        sample.putAll(values);
        metricSamples.add(sample);
        writeMetricsRow(phase, values);
    }

    private void writeMetricsRow(String phase, Map<String, Double> values) {
        try {
            Path report = Paths.get("target",
                    "kf-metrics-" + getClass().getSimpleName() + ".csv");
            Files.createDirectories(report.getParent());
            StringBuilder sb = new StringBuilder();
            if (!metricsHeaderWritten && !Files.exists(report)) {
                sb.append("phase,ts_ms,ts,").append(String.join(",", values.keySet())).append('\n');
            }
            metricsHeaderWritten = true;
            long ts = now();
            sb.append(phase).append(',').append(ts).append(',').append(fmtTs(ts));
            for (double v : values.values()) {
                sb.append(',').append(fmt(v));
            }
            sb.append('\n');
            Files.writeString(report, sb.toString(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** All number→string conversions are Locale.US: '.' is ALWAYS the decimal point, never thousands. */
    private static String fmt(double v) {
        return Double.isNaN(v) ? "" : String.format(Locale.US, "%.3f", v);
    }

    /** All rendered timestamps use {@code yyyy-MM-dd HH:mm:ss.SSS}. */
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static String fmtTs(long epochMs) {
        return TS_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    /**
     * Run an instant PromQL query that returns a labelled vector and collapse it to
     * {@code labelValue → scalar}, keyed by {@code labelKey}. Empty when Prometheus is
     * absent / the query errors. Best-effort.
     */
    protected Map<String, Double> promVector(String promql, String labelKey) {
        Map<String, Double> out = new LinkedHashMap<>();
        if (prometheus == null) return out;
        try {
            String url = promBase() + "/api/v1/query?query="
                    + URLEncoder.encode(promql, StandardCharsets.UTF_8);
            String body = get(url);
            Matcher m = Pattern.compile(
                    "\\{\"metric\":\\{([^}]*)},\"value\":\\[\\s*[0-9.]+\\s*,\"([^\"]*)\"]").matcher(body);
            Pattern lp = Pattern.compile("\"" + Pattern.quote(labelKey) + "\":\"([^\"]*)\"");
            while (m.find()) {
                Matcher lm = lp.matcher(m.group(1));
                String key = lm.find() ? lm.group(1) : "?";
                try {
                    out.put(key, Double.parseDouble(m.group(2)));
                } catch (NumberFormatException ignored) {
                    out.put(key, Double.NaN);
                }
            }
        } catch (RuntimeException ignored) {
            // best-effort
        }
        return out;
    }

    /**
     * Write the consolidated, self-describing markdown report meant to be fed to LLM
     * (e.g. {@code "analyse target/kf-metrics-*.md for bottlenecks"}). Contains: a legend, the
     * per-phase time series, full-run p99 by hot path, and full-run p99 + call-rate by SQL
     * category — the breakdown that separates slow handlers from slow queries. Best-effort and
     * idempotent per run; safe to call from a {@code finally} on both success and failure.
     */
    protected void writeAnalysisReport(String scenario) {
        boolean haveMetrics = prometheus != null && !metricSamples.isEmpty();
        StringBuilder md = new StringBuilder();
        md.append("# kf cluster IT report — ").append(getClass().getSimpleName())
                .append(" (").append(scenario).append(")\n\n");

        // The test timeline is written for every scenario, with or without Prometheus, so the node
        // start/stop moments and the total run length are always captured.
        appendTimeline(md);

        // Capture (and log at INFO) the run TOTALS, then render them — plus any subclass-supplied
        // sections — so they land in the report whether or not Prometheus samples were captured.
        captureTotals();
        appendReportSections(md);

        if (!haveMetrics) {
            md.append("_No Prometheus samples were captured for this scenario; the test timeline ")
                    .append("above is the whole report._\n");
            writeReportFile(md.toString());
            return;
        }

        int w = (int) Math.max(30, (now() - metricsRunStartMs) / 1000);
        md.append("Generated by the cluster IT from a Prometheus scrape of all three nodes. ")
                .append("Feed this file to LLM, e.g. *\"analyse these kf framework metrics ")
                .append("for performance bottlenecks\"*. All latencies are milliseconds; rates are ")
                .append("per second, summed across nodes. Full-run windows below cover ~")
                .append(w).append("s.\n\n");

        md.append("## Legend\n\n")
                .append("- `cmd_*` — command handler (`@CommandHandler`) including OCC retries\n")
                .append("- `append_*` — event-store append of emitted events; `append_lock`/`append_commit` ")
                .append("are phases INSIDE the append (segment-counter `SELECT ... FOR UPDATE` wait, commit/fsync)\n")
                .append("- `dispatch_*` — projection event-handler invocations\n")
                .append("- `tail_read_*` — pull-pump `loadSegmentsTail` reads\n")
                .append("- `sql_*` — raw JDBC time through the `Db` wrapper, by `verb:table` category\n")
                .append("- `dlq_*` — events dead-lettered\n")
                .append("- `conn_acquire_*` / `pool_*` — HikariCP connection pool (acquire wait, pending threads)\n")
                .append("- `pump_lag_*` — pull-pump backlog in events (head of segment counter minus checkpoint)\n")
                .append("- `nudge_*` — append-side pump wakeups (deferred = waited for the boundary commit)\n\n")
                .append("All numbers are Locale.US: `.` is the decimal point, `,` would be thousands. ")
                .append("Timestamps are `yyyy-MM-dd HH:mm:ss.SSS`.\n\n");

        md.append("## Per-phase time series\n\n");
        List<String> cols = new ArrayList<>(METRIC_QUERIES.keySet());
        md.append("| phase | ").append(String.join(" | ", cols)).append(" |\n");
        md.append("|").append("---|".repeat(cols.size() + 1)).append("\n");
        for (Map<String, Object> s : metricSamples) {
            md.append("| ").append(s.get("phase"));
            for (String c : cols) {
                Object v = s.get(c);
                md.append(" | ").append(v instanceof Double d ? fmt(d) : String.valueOf(v));
            }
            md.append(" |\n");
        }
        md.append("\n");

        md.append("## Full-run p99 latency by hot path (over ~").append(w).append("s)\n\n");
        md.append("| hot path | p99_ms |\n|---|---|\n");
        appendP99(md, "command.handle", "kf_command_handle_seconds_bucket", w);
        appendP99(md, "events.append", "kf_events_append_seconds_bucket", w);
        appendP99(md, "segment.tail.read", "kf_segment_tail_read_seconds_bucket", w);
        appendP99(md, "aggregate.rehydrate", "kf_aggregate_rehydrate_seconds_bucket", w);
        appendP99(md, "sql.execute (all)", "kf_sql_execute_seconds_bucket", w);
        md.append("\n");

        appendAppendPhaseBreakdown(md, w);
        appendSaturationSection(md);
        appendTraceSection(md);

        md.append("## Full-run SQL by category (over ~").append(w).append("s)\n\n");
        Map<String, Double> sqlP99 = promVector(String.format(
                "histogram_quantile(0.99, sum(rate(kf_sql_execute_seconds_bucket[%ds])) by (le, category))*1000", w),
                "category");
        Map<String, Double> sqlRate = promVector(String.format(
                "sum(rate(kf_sql_execute_seconds_count[%ds])) by (category)", w), "category");
        md.append("| category | p99_ms | calls/s |\n|---|---|---|\n");
        for (String cat : sqlRate.keySet().stream().sorted().toList()) {
            md.append("| ").append(cat).append(" | ").append(fmt(sqlP99.getOrDefault(cat, Double.NaN)))
                    .append(" | ").append(fmt(sqlRate.get(cat))).append(" |\n");
        }
        md.append("\n");

        md.append("## Dispatch & DLQ by group (over ~").append(w).append("s)\n\n");
        Map<String, Double> dispatchRate = promVector(String.format(
                "sum(rate(kf_event_dispatch_seconds_count[%ds])) by (group)", w), "group");
        Map<String, Double> dlqRate = promVector(String.format(
                "sum(rate(kf_dlq_enqueue_total[%ds])) by (group)", w), "group");
        md.append("| group | dispatch/s | dlq/s |\n|---|---|---|\n");
        java.util.Set<String> groups = new java.util.TreeSet<>(dispatchRate.keySet());
        groups.addAll(dlqRate.keySet());
        for (String g : groups) {
            md.append("| ").append(g).append(" | ").append(fmt(dispatchRate.getOrDefault(g, Double.NaN)))
                    .append(" | ").append(fmt(dlqRate.getOrDefault(g, 0.0))).append(" |\n");
        }
        md.append("\n");

        writeReportFile(md.toString());
    }

    // ---------------------------------------------------------------------------------------------
    // Run TOTALS — the comparison-friendly block emitted for every scenario that sent commands.
    // Client figures come from the report*() accessors (default: this harness's own login/recordOp
    // bookkeeping; the flood scenarios override them with their LoadGenerator counters); db figures
    // are the live read model at report time. Identical INFO format in the kf and axon ITs so the
    // two runs diff line-for-line.
    // ---------------------------------------------------------------------------------------------

    /** Client-acked op count for the report TOTALS. Default: ops sent via {@link #recordOp}. */
    protected long reportAckedOps() {
        return sentOps.get();
    }

    /** Client grand net for the report TOTALS. Default: signed sum of ops sent via {@link #recordOp}. */
    protected long reportGrandNet() {
        return sentNet.get();
    }

    /** Failed (non-2xx) op count. Default 0 — a failed {@link #recordOp} throws; the flood overrides. */
    protected long reportFailedOps() {
        return 0L;
    }

    /** Distinct users for the report TOTALS. Default: users seen via {@link #login}/{@link #recordOp}. */
    protected long reportExpectedUsers() {
        return sentUsers.size();
    }

    protected long dbOpCount() {
        return singleLong("SELECT COUNT(*) FROM pfm_operation");
    }

    protected long dbNet() {
        return singleLong(
                "SELECT COALESCE(SUM(CASE WHEN op_type = 'IN' THEN amount ELSE -amount END), 0) "
                        + "FROM pfm_operation");
    }

    protected long dbUserCount() {
        return singleLong("SELECT COUNT(*) FROM pfm_user");
    }

    /**
     * Log the consolidated TOTALS block at INFO and mirror it into {@link #totalsForReport} for the
     * markdown report. No-op for scenarios that sent no commands (e.g. {@code ClusterFormationIT}).
     * Best-effort: a DB hiccup leaves the {@code db *} figures at their sentinels, never throws.
     */
    private void captureTotals() {
        if (sentOps.get() == 0) {
            return; // no commands sent in this scenario — nothing to total
        }
        long acked = reportAckedOps();
        long net = reportGrandNet();
        long failed = reportFailedOps();
        long users = reportExpectedUsers();
        LOGGER.info("=== TOTALS [{}] ===", scenario);
        LOGGER.info("  acked ops      : {}", acked);
        LOGGER.info("  grand net      : {}", net);
        LOGGER.info("  failed ops     : {}", failed);
        LOGGER.info("  expected users : {}", users);
        long dbOps = -1L, dbNetTotal = 0L, dbUsers = -1L;
        try {
            dbOps = dbOpCount();
            dbNetTotal = dbNet();
            dbUsers = dbUserCount();
        } catch (RuntimeException e) {
            LOGGER.warn("  (read-model totals unavailable: {})", e.getMessage());
        }
        LOGGER.info("  db op count    : {}", dbOps);
        LOGGER.info("  db net         : {}", dbNetTotal);
        LOGGER.info("  db user count  : {}", dbUsers);
        LOGGER.info("=== END TOTALS [{}] ===", scenario);

        totalsForReport.clear();
        totalsForReport.put("acked ops", acked);
        totalsForReport.put("grand net", net);
        totalsForReport.put("failed ops", failed);
        totalsForReport.put("expected users", users);
        totalsForReport.put("db op count", dbOps);
        totalsForReport.put("db net", dbNetTotal);
        totalsForReport.put("db user count", dbUsers);
    }

    /**
     * Render the captured TOTALS block (and any subclass extras) as markdown in the analysis report,
     * rendered just after the test timeline. Empty for scenarios that sent no commands.
     */
    protected void appendReportSections(StringBuilder md) {
        if (totalsForReport.isEmpty()) {
            return;
        }
        md.append("## Run totals [").append(scenario).append("]\n\n");
        md.append("| metric | value |\n|---|---|\n");
        synchronized (totalsForReport) {
            for (var e : totalsForReport.entrySet()) {
                md.append("| ").append(e.getKey()).append(" | ").append(e.getValue()).append(" |\n");
            }
        }
        md.append("\n");
    }

    /** Render the test timeline (ms-from-start of every recorded event) + the total run duration. */
    private void appendTimeline(StringBuilder md) {
        long totalMs = timeline.isEmpty() ? 0L : timeline.get(timeline.size() - 1).offsetMs();
        md.append("## Test timeline (ms from test start)\n\n");
        if (testStartMs != 0L) {
            md.append("Test started at **").append(fmtTs(testStartMs)).append("**\n\n");
        }
        md.append("| t+ms | t+s | Δms | event |\n|---|---|---|---|\n");
        synchronized (timeline) {
            long prevOffsetMs = 0L;
            for (TimelineEvent e : timeline) {
                long deltaMs = e.offsetMs() - prevOffsetMs;
                md.append("| ").append(e.offsetMs())
                        .append(" | ").append(String.format(Locale.US, "%.3f", e.offsetMs() / 1000.0))
                        .append(" | ").append(deltaMs)
                        .append(" | ").append(e.label()).append(" |\n");
                prevOffsetMs = e.offsetMs();
            }
        }
        md.append("\n**Total test duration: ").append(totalMs).append(" ms (")
                .append(String.format(Locale.US, "%.1f", totalMs / 1000.0)).append(" s)**\n\n");
    }

    private void writeReportFile(String content) {
        try {
            Path report = Paths.get("target", "kf-metrics-" + getClass().getSimpleName() + ".md");
            Files.createDirectories(report.getParent());
            Files.writeString(report, content);
            LOGGER.trace("metrics: wrote report {} ({} timeline events, {} phase samples)",
                    report.toAbsolutePath(), timeline.size(), metricSamples.size());
        } catch (IOException e) {
            LOGGER.warn("metrics: failed to write report: {}", e.getMessage());
        }
    }

    /**
     * The table this instrumentation round exists for: the append wall-clock split
     * into its serialisation points (conn acquire, segment-counter lock wait, OCC
     * read, inserts, counter advance, commit) — the saturating phase is the
     * bottleneck. Canonical phase order, not alphabetical.
     */
    private void appendAppendPhaseBreakdown(StringBuilder md, int w) {
        Map<String, Double> p99 = promVector(String.format(
                "histogram_quantile(0.99, sum(rate(kf_append_phase_seconds_bucket[%ds])) by (le, phase))*1000", w),
                "phase");
        if (p99.isEmpty()) {
            return; // extended metrics off / no appends — keep the report clean
        }
        Map<String, Double> mean = promVector(String.format(
                "(sum(rate(kf_append_phase_seconds_sum[%ds])) by (phase) / "
                        + "sum(rate(kf_append_phase_seconds_count[%ds])) by (phase))*1000", w, w), "phase");
        Map<String, Double> rate = promVector(String.format(
                "sum(rate(kf_append_phase_seconds_count[%ds])) by (phase)", w), "phase");
        md.append("## Append phase breakdown (over ~").append(w).append("s)\n\n");
        md.append("Where the append wall-clock goes. `lock` is the per-segment serialiser ")
                .append("(`SELECT ... FOR UPDATE` on `segment_counter`); `commit` is the fsync; ")
                .append("`boundaryCommit` is the async path's commit.\n\n");
        md.append("| phase | p99_ms | mean_ms | calls/s |\n|---|---|---|---|\n");
        List<String> order = List.of("conn", "lock", "currentMax", "insert", "counter", "commit", "boundaryCommit");
        java.util.Set<String> all = new LinkedHashSet<>(order);
        all.addAll(p99.keySet());
        for (String phase : all) {
            if (!p99.containsKey(phase) && !rate.containsKey(phase)) continue;
            md.append("| ").append(phase)
                    .append(" | ").append(fmt(p99.getOrDefault(phase, Double.NaN)))
                    .append(" | ").append(fmt(mean.getOrDefault(phase, Double.NaN)))
                    .append(" | ").append(fmt(rate.getOrDefault(phase, Double.NaN))).append(" |\n");
        }
        md.append("\n");
        appendCoalesceSection(md, w);
    }

    /**
     * Group-commit effectiveness: how many append requests (and event rows) each
     * batch transaction carried. Mean requests/commit near 1 = appends were
     * uncontended (no batching possible); higher = the coalescer is amortising
     * the fsync — commits/s times mean requests should track the command rate.
     */
    private void appendCoalesceSection(StringBuilder md, int w) {
        double commits = promScalar(String.format("sum(rate(kf_append_coalesce_requests_count[%ds]))", w));
        if (Double.isNaN(commits) || commits == 0.0) {
            return; // pre-coalescer build or no own-connection appends in the window
        }
        double meanReqs = promScalar(String.format(
                "sum(rate(kf_append_coalesce_requests_sum[%ds])) / sum(rate(kf_append_coalesce_requests_count[%ds]))", w, w));
        double maxReqs = promScalar("max(kf_append_coalesce_requests_max)");
        double meanEvents = promScalar(String.format(
                "sum(rate(kf_append_coalesce_events_sum[%ds])) / sum(rate(kf_append_coalesce_events_count[%ds]))", w, w));
        md.append("## Append group commit (over ~").append(w).append("s)\n\n");
        md.append("| metric | value |\n|---|---|\n");
        md.append("| commits/s | ").append(fmt(commits)).append(" |\n");
        md.append("| mean requests/commit | ").append(fmt(meanReqs)).append(" |\n");
        md.append("| max requests/commit | ").append(fmt(maxReqs)).append(" |\n");
        md.append("| mean events/commit | ").append(fmt(meanEvents)).append(" |\n");
        md.append("\n");
    }

    /**
     * Saturation gauges: which resource pins first. In-flight appends pinned at 1
     * per segment = the segment lock fully serialises; pool pending &gt; 0 = the
     * Hikari pool is the queue; growing pump lag = the event side is the laggard.
     */
    private void appendSaturationSection(StringBuilder md) {
        Map<String, Double> lagByGroup = promVector("max(kf_pump_lag) by (group)", "group");
        Map<String, Double> inflight = promVector("max(kf_append_inflight) by (segment)", "segment");
        double poolActive = promScalar("max(hikaricp_connections_active)");
        double poolPending = promScalar("max(hikaricp_connections_pending)");
        double poolMax = promScalar("max(hikaricp_connections_max)");
        boolean havePool = !Double.isNaN(poolActive) || !Double.isNaN(poolPending);
        if (lagByGroup.isEmpty() && inflight.isEmpty() && !havePool) {
            return;
        }
        md.append("## Saturation (instant values at report time)\n\n");
        md.append("| gauge | value |\n|---|---|\n");
        for (var e : new TreeMap<>(inflight).entrySet()) {
            md.append("| append inflight max [segment ").append(e.getKey()).append("] | ")
                    .append(fmt(e.getValue())).append(" |\n");
        }
        for (var e : new TreeMap<>(lagByGroup).entrySet()) {
            md.append("| pump lag max [").append(e.getKey()).append("] | ")
                    .append(fmt(e.getValue())).append(" |\n");
        }
        if (havePool) {
            md.append("| hikari active max | ").append(fmt(poolActive)).append(" |\n");
            md.append("| hikari pending max | ").append(fmt(poolPending)).append(" |\n");
            md.append("| hikari pool size | ").append(fmt(poolMax)).append(" |\n");
        }
        md.append("\n");
    }

    // ---------------------------------------------------------------------------------------------
    // Sampled per-command perf traces (1-in-TRACE_SAMPLE_EVERY, in-memory per node — deliberately
    // never persisted to the measured DB). Harvested over GET /kf/perf-traces, dumped to
    // target/kf-perf-trace-<TestClass>.csv and aggregated into the report. Best-effort: a down
    // node contributes nothing, never fails the scenario.
    // ---------------------------------------------------------------------------------------------

    private record TraceRow(String traceId, String node, String commandType, String stage,
                            int stageSeq, long nanos, long detail, boolean ok, long startedAt) {
    }

    private List<TraceRow> harvestPerfTraces() {
        ObjectMapper mapper = new ObjectMapper();
        List<TraceRow> rows = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            try {
                JsonNode root = mapper.readTree(get(httpBase(i) + "/kf/perf-traces"));
                String node = root.path("node").asText(NODE_IDS[i]);
                long dropped = root.path("dropped").asLong(0);
                if (dropped > 0) {
                    // silent truncation must be visible in the report
                    rows.add(new TraceRow("-", node, "(dropped)", "dropped", 0, 0, dropped, true, 0));
                }
                for (JsonNode t : root.path("traces")) {
                    String traceId = t.path("traceId").asText("");
                    String type = t.path("commandType").asText("");
                    long startedAt = t.path("startedAt").asLong(0);
                    boolean ok = t.path("ok").asBoolean(true);
                    int seq = 0;
                    for (JsonNode s : t.path("stages")) {
                        rows.add(new TraceRow(traceId, node, type, s.path("stage").asText(""),
                                seq++, s.path("nanos").asLong(0), s.path("detail").asLong(0),
                                ok, startedAt));
                    }
                }
            } catch (RuntimeException | IOException e) {
                LOGGER.warn("perf-trace harvest from {} failed: {}", NODE_IDS[i], e.getMessage());
            }
        }
        return rows;
    }

    private void writeTraceCsv(List<TraceRow> rows) {
        try {
            Path csv = Paths.get("target", "kf-perf-trace-" + getClass().getSimpleName() + ".csv");
            Files.createDirectories(csv.getParent());
            StringBuilder sb = new StringBuilder(
                    "trace_id,node,command_type,stage,stage_seq,duration_ms,detail,ok,started_at\n");
            for (TraceRow r : rows) {
                sb.append(r.traceId()).append(',').append(r.node()).append(',')
                        .append(r.commandType()).append(',').append(r.stage()).append(',')
                        .append(r.stageSeq()).append(',')
                        .append(String.format(Locale.US, "%.6f", r.nanos() / 1e6)).append(',')
                        .append(r.detail()).append(',').append(r.ok()).append(',')
                        .append(r.startedAt() == 0 ? "" : fmtTs(r.startedAt())).append('\n');
            }
            Files.writeString(csv, sb.toString());
        } catch (IOException e) {
            LOGGER.warn("perf-trace csv write failed: {}", e.getMessage());
        }
    }

    /**
     * Per-stage aggregate of the sampled command traces + the slowest end-to-end
     * commands with their dominant stage — the per-command attribution the phase
     * histograms (cross-command aggregates) cannot give.
     */
    private void appendTraceSection(StringBuilder md) {
        List<TraceRow> rows;
        try {
            rows = harvestPerfTraces();
        } catch (RuntimeException e) {
            LOGGER.warn("perf-trace harvest failed: {}", e.getMessage());
            return;
        }
        if (rows.isEmpty()) {
            return;
        }
        writeTraceCsv(rows);

        md.append("## Trace stage breakdown (sampled 1-in-").append(TRACE_SAMPLE_EVERY)
                .append(" commands)\n\n")
                .append("Full rows in `target/kf-perf-trace-").append(getClass().getSimpleName())
                .append(".csv`. `total` is the whole sendSync; `append.*` sum to the append; ")
                .append("`detail` is stage-specific (events loaded/inserted, retry attempt...).\n\n");

        // dropped-trace visibility
        rows.stream().filter(r -> r.stage().equals("dropped")).forEach(r ->
                md.append("**WARNING**: node ").append(r.node()).append(" dropped ")
                        .append(r.detail()).append(" traces (ring overflow) — stats below are the tail.\n\n"));

        Map<String, List<TraceRow>> byStage = new TreeMap<>();
        for (TraceRow r : rows) {
            if (r.stage().equals("dropped")) continue;
            byStage.computeIfAbsent(r.stage(), k -> new ArrayList<>()).add(r);
        }
        md.append("| stage | count | mean_ms | p95_ms | max_ms |\n|---|---|---|---|---|\n");
        for (var e : byStage.entrySet()) {
            long[] sorted = e.getValue().stream().mapToLong(TraceRow::nanos).sorted().toArray();
            double mean = java.util.Arrays.stream(sorted).average().orElse(0) / 1e6;
            double p95 = sorted[(int) Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1)] / 1e6;
            double max = sorted[sorted.length - 1] / 1e6;
            md.append("| ").append(e.getKey()).append(" | ").append(sorted.length)
                    .append(" | ").append(fmt(mean)).append(" | ").append(fmt(p95))
                    .append(" | ").append(fmt(max)).append(" |\n");
        }
        md.append("\n");

        // top-10 slowest commands by total, with the dominant stage each
        List<TraceRow> totals = byStage.getOrDefault("total", List.of()).stream()
                .sorted((a, b) -> Long.compare(b.nanos(), a.nanos()))
                .limit(10).toList();
        if (!totals.isEmpty()) {
            Map<String, List<TraceRow>> byTrace = new LinkedHashMap<>();
            for (TraceRow r : rows) {
                byTrace.computeIfAbsent(r.traceId(), k -> new ArrayList<>()).add(r);
            }
            md.append("### Slowest sampled commands\n\n");
            md.append("| started at | node | command | total_ms | dominant stage | stage_ms |\n")
                    .append("|---|---|---|---|---|---|\n");
            for (TraceRow t : totals) {
                TraceRow dominant = byTrace.getOrDefault(t.traceId(), List.of()).stream()
                        .filter(r -> !r.stage().equals("total"))
                        .max(java.util.Comparator.comparingLong(TraceRow::nanos))
                        .orElse(t);
                md.append("| ").append(t.startedAt() == 0 ? "" : fmtTs(t.startedAt()))
                        .append(" | ").append(t.node())
                        .append(" | ").append(t.commandType())
                        .append(" | ").append(fmt(t.nanos() / 1e6))
                        .append(" | ").append(dominant.stage())
                        .append(" | ").append(fmt(dominant.nanos() / 1e6)).append(" |\n");
            }
            md.append("\n");
        }
    }

    private void appendP99(StringBuilder md, String label, String bucketMetric, int w) {
        double p99 = promScalar(String.format(
                "histogram_quantile(0.99, sum(rate(%s[%ds])) by (le))*1000", bucketMetric, w));
        md.append("| ").append(label).append(" | ").append(fmt(p99)).append(" |\n");
    }

    // ---------------------------------------------------------------------------------------------
    // JDBC helpers (assert cluster coordination state directly in MySQL)
    // ---------------------------------------------------------------------------------------------

    protected long now() {
        return System.currentTimeMillis();
    }

    protected Connection db() throws SQLException {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    /** item_id → owner_node (null owner stays null) for every cluster_assignments row. */
    protected Map<Integer, String> segmentOwners() {
        Map<Integer, String> out = new TreeMap<>();
        try (Connection c = db(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT item_id, owner_node FROM cluster_assignments")) {
            while (rs.next()) {
                out.put(rs.getInt(1), rs.getString(2));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /** Nodes whose heartbeat is fresher than {@code STALENESS_WINDOW}. */
    protected int freshHeartbeatCount() {
        return single("SELECT COUNT(*) FROM cluster_nodes WHERE last_heartbeat > ?",
                now() - STALENESS_WINDOW_MS);
    }

    protected boolean heartbeatFresh(String nodeId, long withinMs) {
        return single("SELECT COUNT(*) FROM cluster_nodes WHERE node_id = ? AND last_heartbeat > ?",
                nodeId, now() - withinMs) > 0;
    }

    /** The node currently holding an unexpired leader lock, or {@code null}. */
    protected String currentLeader() {
        try (Connection c = db(); PreparedStatement ps = c.prepareStatement(
                "SELECT owner_node FROM cluster_leader_lock WHERE id = 1 "
                        + "AND owner_node IS NOT NULL AND lease_until > ?")) {
            ps.setLong(1, now());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected int single(String sql, Object... args) {
        try (Connection c = db(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Like {@link #single} but returns a {@code long} — for {@code COUNT(*)} / {@code SUM(...)} over
     * the read model, which a 100-user flood overflows past {@code int}.
     */
    protected long singleLong(String sql, Object... args) {
        try (Connection c = db(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // tiny flat-JSON readers (these responses are flat objects; no JSON lib needed)
    // ---------------------------------------------------------------------------------------------

    protected static boolean jsonBool(String body, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(true|false)").matcher(body);
        return m.find() && "true".equals(m.group(1));
    }

    protected static long jsonLong(String body, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(-?\\d+)").matcher(body);
        if (!m.find()) {
            throw new IllegalStateException("no numeric '" + key + "' in: " + body);
        }
        return Long.parseLong(m.group(1));
    }

    protected static String jsonStr(String body, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"").matcher(body);
        if (!m.find()) {
            throw new IllegalStateException("no string '" + key + "' in: " + body);
        }
        return m.group(1);
    }
}
