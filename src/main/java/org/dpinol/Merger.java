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
    private final ProgressLogger numBytesRead = new ProgressLogger("num bytes read", 10_000_000);
    private final LongAdder linesRead = new LongAdder();
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

    void merge() throws IOException {
        Log.info("Merging " + readers.size() + " files");
        //load heap
        int readerIndex = 0;
        CompletableFuture<FileLine>[] cfs = new CompletableFuture[readers.size()];
        for (AsyncFileLineReader reader : readers) {
            final int index = readerIndex++;
            CompletableFuture<FileLine> cf = reader.read();
            cf.thenAccept(line -> front.add(new LineWithOrigin(line, index)));
            linesPushed.add(1);
        }
        CompletableFuture<Void> frontUpdatedCF = CompletableFuture.allOf(cfs);
        CompletableFuture<Void> lineWrittenCF =  new CompletableFuture<>();
        lineWrittenCF.complete(null);
        while (!front.isEmpty()) {
            frontUpdatedCF.join();
            LineWithOrigin first = front.poll();
            lineWrittenCF = lineWrittenCF.thenCompose( (Void v) ->
                first.line.write(writer, executorService));
            frontUpdatedCF = pushFromReader(first.readerIndex);
        }
        Log.info(linesPushed + " lines pushed");
        Log.info(linesRead + " lines read");
    }

    /**
     * Actually we're not making any use of being async now. Maybe we can read from all files
     * at the same time, so that we have at least BigLine in a cache?
     */
    CompletableFuture<Void> pushFromReader(final int index) {
        final int LOG_STEP_BYTES = 10_000_000;
        AsyncFileLineReader firstReader = readers.get(index);
        return firstReader.read().thenAccept(
                newLine -> {
                    if (newLine != null) {
                        front.add(new LineWithOrigin(newLine, index));
                        linesPushed.add(1);
                        numBytesRead.inc(newLine.getNumBytes());
                    } else {
                        numDrainedFiles.add(1);
                    }
                }
        );
    }

}
