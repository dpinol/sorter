package org.dpinol;

import org.dpinol.util.ProgressLogger;
import org.dpinol.util.ThrowingConsumer;
import org.dpinol.util.ThrowingSupplier;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

/**
 * Merges a list of sorted files into a single one
 */
public class FilesMerger implements AutoCloseable {
    private final ExecutorService executorService;
    final List<ThrowingSupplier<FileLine>> suppliers;
    final FileConsumer consumer;
    private final ProgressLogger numBytesRead = new ProgressLogger("num bytes read", 100_000_000);
    private final LongAdder linesPushed = new LongAdder();
    private final LongAdder numDrainedFiles = new LongAdder();
    private final Merger merger;

    /**
     * @param inputFiles should not be empty
     */
    public FilesMerger(List<File> inputFiles, File output, ExecutorService executorService) throws Exception {
        this.executorService = executorService;
        suppliers = new ArrayList<>(inputFiles.size());
        for (File inputFile : inputFiles) {
            suppliers.add(new FileSupplier(inputFile));
        }
        consumer = new FileConsumer(output);
        merger = new Merger(suppliers, consumer, executorService);
    }

    private static class FileSupplier implements ThrowingSupplier<FileLine>, AutoCloseable {
        private final FileLineReader reader;

        FileSupplier(File input) throws Exception {
            this.reader = new FileLineReader(input);
        }

        @Override
        public FileLine get() throws Exception {
            return reader.readFileLine();
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }

    private static class FileConsumer implements ThrowingConsumer<FileLine>, AutoCloseable {
        private final BufferedWriter writer;

        FileConsumer(File output) throws Exception {
            //TODO use FileChannel?
            writer = new BufferedWriter(new FileWriter(output));
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }

        @Override
        public void accept(FileLine fileLine) throws Exception {
            fileLine.write(writer);
        }
    }

    @Override
    public void close() throws Exception {
        for (ThrowingSupplier supplier : suppliers) {
            ((AutoCloseable) supplier).close();
        }
        consumer.close();
    }


    void merge() throws Exception {
        merger.merge();
    }


}
