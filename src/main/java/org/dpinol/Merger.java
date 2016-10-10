package org.dpinol;

import org.dpinol.util.Log;
import org.dpinol.util.ProgressLogger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

/**
 * Merges a list of sorted files into a single one
 */
public class Merger implements AutoCloseable {
    private final List<AsyncFileLineReader> readers;
    private final ExecutorService executorService;
    private final BufferedWriter writer;
    //to avoid comparing the first of each file too many times, we use a heap
    private final SimpleHeap<LineWithOrigin> front;
    private final ProgressLogger numBytesRead = new ProgressLogger("num bytes read", 100_000_000);
    private final LongAdder linesPushed = new LongAdder();
    private final LongAdder numDrainedFiles = new LongAdder();

    /**
     * @param inputFiles should not be empty
     */
    public Merger(List<File> inputFiles, File output, ExecutorService executorService) throws IOException {
        readers = new ArrayList<>(inputFiles.size());
        this.executorService = executorService;
        for (File inputFile : inputFiles) {
            if (inputFile.length() > 0) {
                readers.add(new AsyncFileLineReader(inputFile));
            }
        }
        front = new SimpleHeap<>(readers.size());
        writer = new BufferedWriter(new FileWriter(output));
    }

    @Override
    public void close() throws Exception {
        for (AsyncFileLineReader reader : readers) {
            reader.close();
        }
        writer.close();
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
        Log.info("Merging " + readers.size() + " files");
        //load heap
        int readerIndex = 0;
        CompletableFuture<Void>[] cfs = new CompletableFuture[readers.size()];
        Log.info("%d readers", readers.size());
        for (AsyncFileLineReader reader : readers) {
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
                    first.line.write(writer);
                } catch (IOException e) {
                    //TODO manager
                    Log.error(e.toString());
                }
            });
            pushFromReader(first.readerIndex);
        }
        lineWrittenCF.join();
        Log.info(linesPushed + " lines pushed");
        Log.info(numDrainedFiles + " files drained");
    }


    void pushFromReader(final int index) throws Exception {
        final AsyncFileLineReader firstReader = readers.get(index);
        FileLine fileLine = firstReader.readLine();
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
