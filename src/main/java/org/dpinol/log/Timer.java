package org.dpinol.log;

import java.io.IOException;

/**
 * RIIA which logs how long in ms did the object live
 */
public class Timer implements AutoCloseable {
    private static final Log logger = new Log(Timer.class);

    private final long start = System.currentTimeMillis();

    @Override
    public void close() throws IOException {
        logger.info("Timer took " + (System.currentTimeMillis() - start) + "ms");
    }
}
