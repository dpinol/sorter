package org.dpinol.helpers;

import java.io.Closeable;
import java.io.IOException;

import org.dpinol.PerformanceTest;
import org.dpinol.util.Log;

/**
 * Created by dani on 29/09/16.
 */
public class Timer implements Closeable {
    private static final Log logger = new Log(PerformanceTest.class);

    private final long start = System.currentTimeMillis();

    @Override
    public void close() throws IOException {
        logger.info("Spent " + (System.currentTimeMillis() - start) + "ms");
    }
}
