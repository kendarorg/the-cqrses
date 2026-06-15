package org.kendar.axonpfm.it;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Axon counterpart of the kf {@code AbstractClusterIT}: one MySQL container (alias {@code kfdb}) and
 * three real JVMs of {@code axon-spring-app} (aliases {@code node1..3}) on one Docker network, in
 * cluster mode against that one MySQL. Same Testcontainers substrate so the scenarios compare
 * head-to-head; the differences are all on the coordination layer:
 *
 * <ul>
 *   <li><b>No liveness server.</b> Axon has no {@code /alive} endpoint, so nodes are waited on (and
 *       probed for liveness) purely through {@code /cluster/status}.</li>
 *   <li><b>Ownership via the app, not a coordination table.</b> Segment ownership is read from each
 *       node's {@code GET /cluster/segments} (which segments its streaming processors currently claim)
 *       and unioned into a segment→node map — robust and framework-honest. The shared
 *       {@code token_entry} table is available as a DB cross-check.</li>
 *   <li><b>No leader.</b> Server-less Axon distributes work by peer-to-peer token stealing; there is
 *       no leader lock to assert.</li>
 *   <li><b>Timings keyed to the token claim timeout</b> (Axon's analog of kf's lease/staleness):
 *       a dead node's claims become stealable after ~{@link #CLAIM_TIMEOUT_MS}.</li>
 * </ul>
 *
 * <p>Because {@code axon-spring-app} emits the SAME {@code kf.*} meters as the kf app, the Prometheus
 * capture + analysis report below are the kf machinery essentially unchanged. Docker-gated.
 *
 * <p><b>Port map</b> (host:container): node{i}: app 1808{i+1}, jdwp 1500{5+i}.
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractClusterIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger("cluster-it");

    protected static final int SEGMENTS = 6;
    protected static final int NODE_COUNT = 3;

    static final int APP_PORT = 8080;
    static final int JDWP_PORT = 5005;

    static final int[] APP_HOST_PORTS = {18081, 18082, 18083};
    static final int[] JDWP_HOST_PORTS = {15005, 15006, 15007};

    protected static final String[] NODE_IDS = {"node1", "node2", "node3"};

    /** Axon token claim timeout (mirrors AxonTokenStoreConfig.CLAIM_TIMEOUT). A dead node's claims
     *  are stealable after roughly this long without renewal. */
    protected static final long CLAIM_TIMEOUT_MS = 10_000L;

    private static final String APP_DB_URL =
            "jdbc:mysql://kfdb:3306/kf?user=kf&password=kf&allowPublicKeyRetrieval=true&useSSL=false";

    private static final String DOCKERFILE = """
            FROM eclipse-temurin:25-jre
            COPY app.jar /app/app.jar
            ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]
            """;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    static final int PROM_PORT = 9090;

    protected static Network network;
    protected static MySQLContainer<?> mysql;
    protected static final List<GenericContainer<?>> nodes = new ArrayList<>();
    protected static GenericContainer<?> prometheus;

    @BeforeAll
    static void startCluster() {
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
        for (int i = 0; i < NODE_COUNT; i++) {
            LOGGER.trace("starting {}", NODE_IDS[i]);
            nodes.get(i).start();
        }

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
            LOGGER.warn("prometheus failed to start; metric capture disabled: {}", e.getMessage());
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
        return new ImageFromDockerfile("axon-pfm-node:it", false)
                .withFileFromPath("app.jar", bootJar)
                .withFileFromString("Dockerfile", DOCKERFILE);
    }

    private static Path locateBootJar() {
        Path targetDir = Paths.get("..", "axon-spring-app", "target").toAbsolutePath().normalize();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "axon-spring-app-*.jar")) {
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
        throw new IllegalStateException("axon-spring-app boot jar not found in " + targetDir
                + " — run `mvn -pl kf-samples/axon-spring-app -am package` first");
    }

    private static GenericContainer<?> newNode(int i, ImageFromDockerfile image) {
        GenericContainer<?> node = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(NODE_IDS[i])
                .withExposedPorts(APP_PORT, JDWP_PORT)
                .withEnv("PFM_DATASOURCE_URL", APP_DB_URL)
                .withEnv("PFM_CLUSTER_MODE", "true")
                .withEnv("PFM_SEGMENTS", String.valueOf(SEGMENTS))
                .withEnv("PFM_CLUSTER_NODE_ID", NODE_IDS[i])
                .withEnv("PFM_CLUSTER_HOST", NODE_IDS[i])
                .withEnv("SERVER_PORT", String.valueOf(APP_PORT))
                .withEnv("SPRING_OUTPUT_ANSI_ENABLED", "NEVER")
                .withEnv("JAVA_OPTS", javaOpts(i))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(NODE_IDS[i])))
                .waitingFor(Wait.forHttp("/cluster/status").forPort(APP_PORT).forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(180)));
        node.setPortBindings(List.of(
                APP_HOST_PORTS[i] + ":" + APP_PORT,
                JDWP_HOST_PORTS[i] + ":" + JDWP_PORT));
        node.dependsOn(mysql);
        return node;
    }

    private static String javaOpts(int i) {
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

    protected void login(int nodeIdx, String user) {
        post(httpBase(nodeIdx) + "/api/login", "{\"username\":\"" + user + "\"}");
        sentUsers.add(user);
    }

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
    // Cluster-state views (app endpoint authoritative; token_entry as DB cross-check)
    // ---------------------------------------------------------------------------------------------

    /**
     * segment → owning node, built by asking every reachable+running node which segments its streaming
     * processors currently claim ({@code GET /cluster/segments}) and inverting. A stopped/unreachable
     * node contributes nothing. During a handoff two nodes may briefly both report a segment; the
     * scenarios assert the settled state via Awaitility.
     */
    protected Map<Integer, String> segmentOwners() {
        Map<Integer, String> out = new TreeMap<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int seg : ownedSegmentsOf(i)) {
                out.put(seg, NODE_IDS[i]);
            }
        }
        return out;
    }

    /** Segments node {@code i} currently owns, or empty if it is down / not running / unreachable. */
    protected List<Integer> ownedSegmentsOf(int nodeIdx) {
        List<Integer> segs = new ArrayList<>();
        try {
            if (!clusterRunning(nodeIdx)) {
                return segs;
            }
            String body = get(httpBase(nodeIdx) + "/cluster/segments");
            Matcher m = Pattern.compile("-?\\d+").matcher(body);
            while (m.find()) {
                segs.add(Integer.parseInt(m.group()));
            }
        } catch (RuntimeException e) {
            // node down / unreachable → owns nothing
        }
        return segs;
    }

    /** Number of nodes that are reachable and report {@code running:true}. (Liveness, HTTP-based.) */
    protected int liveNodeCount() {
        int n = 0;
        for (int i = 0; i < NODE_COUNT; i++) {
            try {
                if (clusterRunning(i)) {
                    n++;
                }
            } catch (RuntimeException ignored) {
                // unreachable
            }
        }
        return n;
    }

    /** Whether {@code nodeId} is reachable and running. */
    protected boolean nodeLive(String nodeId) {
        for (int i = 0; i < NODE_COUNT; i++) {
            if (NODE_IDS[i].equals(nodeId)) {
                try {
                    return clusterRunning(i);
                } catch (RuntimeException e) {
                    return false;
                }
            }
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------------
    // JDBC helpers (read model + token_entry cross-check)
    // ---------------------------------------------------------------------------------------------

    protected long now() {
        return System.currentTimeMillis();
    }

    protected Connection db() throws SQLException {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    /** DB cross-check: segment → owner for one processor's token_entry rows (owner = node id). */
    protected Map<Integer, String> tokenOwners(String processorName) {
        Map<Integer, String> out = new TreeMap<>();
        try (Connection c = db();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT segment, owner FROM token_entry WHERE processor_name = ?")) {
            ps.setString(1, processorName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getInt(1), rs.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

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
    // Prometheus / metrics capture (best-effort — never fails a scenario). The axon-spring-app emits
    // the SAME kf.* meters as the kf app, so these queries are the kf report machinery unchanged
    // (only sql_*_entry_rate points at Axon's domain_event_entry table).
    // ---------------------------------------------------------------------------------------------

    private static final Map<String, String> METRIC_QUERIES = new LinkedHashMap<>() {{
        put("cmd_rate_per_s", "sum(rate(kf_command_handle_seconds_count[%ds]))");
        put("cmd_p99_ms", "histogram_quantile(0.99, sum(rate(kf_command_handle_seconds_bucket[%ds])) by (le))*1000");
        put("append_p99_ms", "histogram_quantile(0.99, sum(rate(kf_events_append_seconds_bucket[%ds])) by (le))*1000");
        put("dispatch_rate_per_s", "sum(rate(kf_event_dispatch_seconds_count[%ds]))");
        put("tail_read_p99_ms", "histogram_quantile(0.99, sum(rate(kf_segment_tail_read_seconds_bucket[%ds])) by (le))*1000");
        put("sql_p99_ms", "histogram_quantile(0.99, sum(rate(kf_sql_execute_seconds_bucket[%ds])) by (le))*1000");
        put("sql_domain_event_rate_per_s", "sum(rate(kf_sql_execute_seconds_count{category=\"select:domain_event_entry\"}[%ds]))");
        put("dlq_rate_per_s", "sum(rate(kf_dlq_enqueue_total[%ds]))");
    }};

    private boolean metricsHeaderWritten = false;
    private final List<Map<String, Object>> metricSamples = new ArrayList<>();
    private long metricsRunStartMs = 0L;

    protected String promBase() {
        return "http://localhost:" + prometheus.getMappedPort(PROM_PORT);
    }

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
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("phase", phase);
        sample.put("ts_ms", now());
        sample.putAll(values);
        metricSamples.add(sample);
        writeMetricsRow(phase, values);
    }

    private void writeMetricsRow(String phase, Map<String, Double> values) {
        try {
            Path report = Paths.get("target", "axon-metrics-" + getClass().getSimpleName() + ".csv");
            Files.createDirectories(report.getParent());
            StringBuilder sb = new StringBuilder();
            if (!metricsHeaderWritten && !Files.exists(report)) {
                sb.append("phase,ts_ms,").append(String.join(",", values.keySet())).append('\n');
            }
            metricsHeaderWritten = true;
            sb.append(phase).append(',').append(now());
            for (double v : values.values()) {
                sb.append(',').append(fmt(v));
            }
            sb.append('\n');
            Files.writeString(report, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String fmt(double v) {
        return Double.isNaN(v) ? "" : String.format("%.3f", v);
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

    protected void writeAnalysisReport(String scenario) {
        boolean haveMetrics = prometheus != null && !metricSamples.isEmpty();
        StringBuilder md = new StringBuilder();
        md.append("# axon cluster IT report — ").append(getClass().getSimpleName())
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
        md.append("Generated by the axon cluster IT from a Prometheus scrape of all three nodes. ")
                .append("The axon-spring-app emits the same `kf.*` meters as the kf app, so this is ")
                .append("directly comparable to the kf report. All latencies are milliseconds; rates are ")
                .append("per second, summed across nodes. Full-run windows below cover ~")
                .append(w).append("s.\n\n");

        md.append("## Legend\n\n")
                .append("- `cmd_*` — command handler (`@CommandHandler`) including OCC retries\n")
                .append("- `append_*` — event-store append of emitted events\n")
                .append("- `dispatch_*` — projection event-handler invocations\n")
                .append("- `tail_read_*` — pull-pump `loadSegmentsTail` reads\n")
                .append("- `sql_*` — raw JDBC time through the `Db` wrapper, by `verb:table` category\n")
                .append("- `dlq_*` — events dead-lettered\n\n");

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
        md.append("| t+ms | t+s | Δms | event |\n|---|---|---|---|\n");
        synchronized (timeline) {
            long prevOffsetMs = 0L;
            for (TimelineEvent e : timeline) {
                long deltaMs = e.offsetMs() - prevOffsetMs;
                md.append("| ").append(e.offsetMs())
                        .append(" | ").append(String.format("%.3f", e.offsetMs() / 1000.0))
                        .append(" | ").append(deltaMs)
                        .append(" | ").append(e.label()).append(" |\n");
                prevOffsetMs = e.offsetMs();
            }
        }
        md.append("\n**Total test duration: ").append(totalMs).append(" ms (")
                .append(String.format("%.1f", totalMs / 1000.0)).append(" s)**\n\n");
    }

    private void writeReportFile(String content) {
        try {
            Path report = Paths.get("target", "axon-metrics-" + getClass().getSimpleName() + ".md");
            Files.createDirectories(report.getParent());
            Files.writeString(report, content);
            LOGGER.trace("metrics: wrote report {} ({} timeline events, {} phase samples)",
                    report.toAbsolutePath(), timeline.size(), metricSamples.size());
        } catch (IOException e) {
            LOGGER.warn("metrics: failed to write report: {}", e.getMessage());
        }
    }

    private void appendP99(StringBuilder md, String label, String bucketMetric, int w) {
        double p99 = promScalar(String.format(
                "histogram_quantile(0.99, sum(rate(%s[%ds])) by (le))*1000", bucketMetric, w));
        md.append("| ").append(label).append(" | ").append(fmt(p99)).append(" |\n");
    }

    // ---------------------------------------------------------------------------------------------
    // tiny flat-JSON readers
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
