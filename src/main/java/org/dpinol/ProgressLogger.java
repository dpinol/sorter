package org.dpinol;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Threadsafe (almost) accumulator which logs progress at specified intervals
 * Created by dani on 08/10/2016.
 */
class ProgressLogger {
    private final String name;
    private final long logInterval;
    private long nextLog;
    private final AtomicLong current = new AtomicLong();

    ProgressLogger(String name, long logInterval) {
        this.name = name;
        this.logInterval = logInterval;
        this.nextLog = logInterval;
    }

    void inc(long inc) {
        long lastVal = current.longValue();
        if (current.addAndGet(inc) > nextLog && lastVal < nextLog) {
            nextLog += logInterval;
            Global.log(name + " reached " + current);
        }
    }
}
