package org.dpinol;

import org.dpinol.util.Log;
import org.dpinol.util.ProgressLogger;
import org.dpinol.util.ThrowingConsumer;
import org.dpinol.util.ThrowingSupplier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Merges a list of "sorted lines suppliers" (can provide from a file, or for a temporary list of lines in memory)
 * into a single sorted stream of lines which is provided to a consumer
 */
public class Merger implements AutoCloseable {
    private final Log logger;

    private final List<ThrowingSupplier<FileLine>> suppliers;
    private final ThrowingConsumer<FileLine> consumer;
    private final ExecutorService executorService;
    //to avoid comparing the first of each file too many times, we use a heap
    private final SimpleHeap<LineWithOrigin> front;
    private static final ProgressLogger numBytesRead = new ProgressLogger("num bytes read", 100_000_000);
    private final LongAdder linesPushed = new LongAdder();
    private final LongAdder numDrainedFiles = new LongAdder();

    /**
     *
     * @param suppliers lines must be sorted ascending
     * @param consumer lines will be supplied sorted √
     * @param executorService
     * @throws IOException
     */
    public Merger(List<ThrowingSupplier<FileLine>> suppliers, ThrowingConsumer<FileLine> consumer, ExecutorService executorService) throws IOException {
        logger = new Log(suppliers.size() + " suppliers to a " + consumer.getClass().getSimpleName());
        this.suppliers = suppliers;
        this.consumer = consumer;
        this.executorService = executorService;
        front = new SimpleHeap<>(suppliers.size());
    }

    @Override
    public void close() throws Exception {
    }


    private static class LineWithOrigin implements Comparable<LineWithOrigin> {
        FileLine line;
        int readerIndex;

        LineWithOrigin(FileLine line, int readerIndex) {
            this.line = line;
            this.readerIndex = readerIndex;
        }

        @Override
        public int compareTo(LineWithOrigin o) {
            return line.compareTo(o.line);
        }
    }

    void merge() throws Exception {
        logger.info("Merging " + suppliers.size() + " inputs");
        //load heap
        int readerIndex = 0;
        for (ThrowingSupplier<FileLine> supplier: suppliers) {
            pushFromReader(readerIndex);
            readerIndex++;
            linesPushed.add(1);
        }
        CompletableFuture<Void> lineWrittenCF = CompletableFuture.runAsync(() -> {
        }, executorService);
        while (!front.isEmpty()) {
            LineWithOrigin first = front.poll();
            lineWrittenCF = lineWrittenCF.thenRun(() ->
            {
                try {
                    consumer.accept(first.line);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            pushFromReader(first.readerIndex);
        }
        lineWrittenCF.join();
        logger.info(linesPushed + " lines pushed");
        logger.info(numDrainedFiles + " files drained");
    }

    void pushFromReader(final int index) throws Exception {
        final ThrowingSupplier<FileLine> supplier = suppliers.get(index);
        FileLine fileLine = supplier.get();
        if (fileLine != null) {
            LineWithOrigin lineWithOrigin = new LineWithOrigin(fileLine, index);
            front.add(lineWithOrigin);
            linesPushed.add(1);
            numBytesRead.inc(fileLine.getNumBytes());
        } else {
            numDrainedFiles.add(1);
        }
    }


}
