package org.dpinol;

import java.io.Closeable;
import java.io.IOException;

import static org.dpinol.Global.log;

/**
 * Created by dani on 29/09/16.
 */
public class Timer implements Closeable {
    long start = System.currentTimeMillis();

    @Override
    public void close() throws IOException {
        log("Spent " + (System.currentTimeMillis() - start) + "ms");
    }
}
