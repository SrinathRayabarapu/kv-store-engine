package com.kvstore.engine;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically evaluates {@link Compactor#shouldCompact()} and runs {@link BitcaskEngine#compact(Compactor)}
 * on a single background thread. Each Raft (or standalone) node runs its own scheduler — compaction is local.
 */
public final class CompactionScheduler implements Closeable {

    private static final Logger LOG = Logger.getLogger(CompactionScheduler.class.getName());

    private final BitcaskEngine engine;
    private final Compactor compactor;
    private final ScheduledExecutorService scheduler;

    /**
     * @param intervalSec fixed delay between checks; must be {@code > 0}
     */
    public CompactionScheduler(BitcaskEngine engine, Compactor compactor, long intervalSec) {
        if (intervalSec <= 0) {
            throw new IllegalArgumentException("intervalSec must be positive");
        }
        this.engine = engine;
        this.compactor = compactor;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kv-compaction");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::tick, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    private void tick() {
        try {
            if (engine.shouldCompact(compactor)) {
                engine.compact(compactor);
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Scheduled compaction failed", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }
}
