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
    private final List<FileLineReader> readers;
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
                readers.add(new FileLineReader(inputFile));
            }
        }
        front = new SimpleHeap<>(readers.size());
        writer = new BufferedWriter(new FileWriter(output));
    }

    @Override
    public void close() throws Exception {
        for (FileLineReader reader : readers) {
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
        CompletableFuture<Void>[] cfs = new CompletableFuture[readers.size()];
        for (FileLineReader reader : readers) {
            cfs[readerIndex] = pushFromReaderAsync(readerIndex);
            readerIndex++;
            linesPushed.add(1);
        }
        CompletableFuture<Void> frontUpdatedCF = CompletableFuture.allOf(cfs);
        CompletableFuture<Void> lineWrittenCF = CompletableFuture.runAsync(() -> {
        }, executorService);
        frontUpdatedCF.join();
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
        Log.info(linesRead + " lines read");
        Log.info(numDrainedFiles + " files drained");
    }

    /**
     * Actually we're not making any use of being async now. Maybe we can read from all files
     * at the same time, so that we have at least BigLine in a cache?
     */
    CompletableFuture<Void> pushFromReaderAsync(final int index) {
        final FileLineReader firstReader = readers.get(index);
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                FileLine fileLine = firstReader.readFileLine();
                if (fileLine != null) {
                    LineWithOrigin lineWithOrigin = new LineWithOrigin(fileLine, index);
                    synchronized (this) {
                        front.add(lineWithOrigin);
                    }
                    linesPushed.add(1);
                    numBytesRead.inc(fileLine.getNumBytes());
                } else {
                    numDrainedFiles.add(1);
                }
                cf.complete(null);
            } catch (IOException e) {
                cf.completeExceptionally(e);
            }
        }, executorService);
        return cf;
    }

    void pushFromReader(final int index) throws IOException {
        final FileLineReader firstReader = readers.get(index);
        FileLine fileLine = firstReader.readFileLine();
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
