package org.dpinol;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import static org.dpinol.Global.BUFFER_SIZE;
import static org.dpinol.Global.LINE_SEPARATOR;

/**
 * Merges a list of sorted files into a single one
 */
public class Merger implements AutoCloseable {
    private final List<AsyncFileLineReader> readers;
    private final BufferedWriter writer;
    //to avoid comparing the first of each file too many times, we use a heap
    private final SimpleHeap<LineWithOrigin> front;
    private final LongAdder linesRead = new LongAdder();
    private final LongAdder linesPushed = new LongAdder();

    /**
     * @param inputFiles should not be empty
     */
    public Merger(List<File> inputFiles, File output) throws IOException {
        readers = new ArrayList<>(inputFiles.size());
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
        Global.log("Merging " + readers.size() + " files");
        //load heap
        int readerIndex = 0;
        CompletableFuture<FileLine>[] cfs = new CompletableFuture[readers.size()];
        for (AsyncFileLineReader reader : readers) {
            final int index = readerIndex++;
            CompletableFuture<FileLine> cf = reader.read();
            cf.thenAccept(line -> front.add(new LineWithOrigin(line, index)));
            linesPushed.add(1);
        }
        CompletableFuture.allOf(cfs).join();

        int numDrainedFiles = 0;
        int linesRead = 0;
        int logStep = Math.max(readers.size() / 10, 1);
        while (!front.isEmpty()) {
            LineWithOrigin first = front.poll();
            first.line.write(writer, true);
            pushNext();
        }
        Global.log(linesPushed + " lines pushed");
        Global.log(linesRead + " lines read");
    }

    void pushFromReader(final int index) {
        AsyncFileLineReader firstReader = readers.get(index);
        CompletableFuture<FileLine> cf = firstReader.read();
        cf.thenAccept(
                newLine -> {
                    if (newLine != null) {
                        front.add(new LineWithOrigin(newLine, index));
                        linesPushed.add(1);
                    } else {
                        numDrainedFiles++;
                        if (numDrainedFiles % logStep == 0) {
                            Global.log("Completed " + numDrainedFiles + "/" + readers.size());
                        }
                    }
                }
    }

}
