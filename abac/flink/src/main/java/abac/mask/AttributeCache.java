package abac.mask;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Values;

/**
 * Local field and clearance cache. A failed refresh keeps the last good
 * snapshot. The task does not start until the first load succeeds.
 */
final class AttributeCache implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(AttributeCache.class.getName());

    private final String topic;
    private final String principal;
    private final int refreshSeconds;
    private final Driver driver;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile Map<String, String> fields = Map.of();
    private volatile int clearance = FieldMasker.UNKNOWN_CLEARANCE;
    private Thread refresher;

    AttributeCache(String uri, String user, String password, String topic, String principal, int refreshSeconds) {
        this.topic = topic;
        this.principal = principal;
        this.refreshSeconds = refreshSeconds;
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    void start() {
        loadWithRetry();
        refresher = new Thread(this::refreshLoop, "attribute-cache");
        refresher.setDaemon(true);
        refresher.start();
    }

    Map<String, String> fields() {
        return fields;
    }

    int clearance() {
        return clearance;
    }

    private void refreshLoop() {
        while (running.get()) {
            try {
                TimeUnit.SECONDS.sleep(refreshSeconds);
                if (running.get()) {
                    loadOnce();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception ex) {
                LOG.log(Level.WARNING, "attribute refresh failed; keeping the last snapshot", ex);
            }
        }
    }

    private void loadWithRetry() {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                loadOnce();
                return;
            } catch (RuntimeException ex) {
                last = ex;
                LOG.log(Level.WARNING, "attribute load attempt " + attempt + " failed", ex);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while loading attributes", interrupted);
                }
            }
        }
        throw new IllegalStateException("could not load field attributes from Neo4j", last);
    }

    private void loadOnce() {
        try (Session session = driver.session()) {
            Map<String, String> nextFields = new HashMap<>();
            Result fieldResult = session.run(
                    "MATCH (t:Topic {name: $topic})-[:HAS_FIELD]->(f:Field) "
                            + "RETURN f.name AS name, f.sensitivity AS sensitivity",
                    Values.parameters("topic", topic));
            while (fieldResult.hasNext()) {
                Record record = fieldResult.next();
                if (record.get("name").isNull() || record.get("sensitivity").isNull()) {
                    continue;
                }
                nextFields.put(record.get("name").asString(), record.get("sensitivity").asString());
            }

            int nextClearance = FieldMasker.UNKNOWN_CLEARANCE;
            Result principalResult = session.run(
                    "MATCH (p:Principal {name: $name}) RETURN p.clearance AS clearance",
                    Values.parameters("name", principal));
            if (principalResult.hasNext()) {
                Record record = principalResult.next();
                if (!record.get("clearance").isNull()) {
                    nextClearance = record.get("clearance").asInt();
                }
            }

            fields = Collections.unmodifiableMap(nextFields);
            clearance = nextClearance;
            LOG.info("cached " + nextFields.size() + " fields for " + topic
                    + "; principal " + principal + " clearance " + nextClearance);
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (refresher != null) {
            refresher.interrupt();
        }
        driver.close();
    }
}
