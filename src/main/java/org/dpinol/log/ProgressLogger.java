package org.dpinol.log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Threadsafe accumulator which logs progress at specified intervals
 */
public class ProgressLogger {
    private final static Log logger = new Log(ProgressLogger.class);

    private final String name;
    private final long logInterval;
    private long nextLog;
    private final AtomicLong current = new AtomicLong();

    public ProgressLogger(String name, long logInterval) {
        this.name = name;
        this.logInterval = logInterval;
        this.nextLog = logInterval;
    }

    public void inc(long inc) {
        long lastVal = current.longValue();
        if (current.addAndGet(inc) > nextLog && lastVal < nextLog) {
            nextLog += logInterval;
            logger.info("%s reached %d", name, current.get());
        }
    }
}
