package org.dpinol;

import org.dpinol.util.Log;
import org.dpinol.util.ThrowingConsumer;
import org.dpinol.util.ThrowingSupplier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Merges a list of sorted files into a single one.
 * - NUM_THREADS threads merge each one segment of the input files into a queue
 * - main thread reads from both queues and merges into the output file
 */
public class ParallelFilesMerger implements AutoCloseable {
    private final static Log logger = new Log(ParallelFilesMerger.class);

    private final static int NUM_THREADS = 2;
    private final static int QUEUE_LEN = 1000;
    private final List<File> inputFiles;
    private final File output;
    private final ExecutorService executorService;
    private final List<ArrayBlockingQueue<FileLine>> queues = new ArrayList<>(NUM_THREADS);
    private final AtomicLong numInputMergersFinished = new AtomicLong(0);

    /**
     * @param inputFiles should have at least one item
     */
    public ParallelFilesMerger(List<File> inputFiles, File output, ExecutorService executorService) throws Exception {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("no inputFiles passed");
        }
        this.inputFiles = inputFiles.stream().filter((f) -> f.length() != 0).collect(Collectors.toList());
        this.output = output;
        this.executorService = executorService;
        for (File inputFile : this.inputFiles) {
            queues.add(new ArrayBlockingQueue<>(QUEUE_LEN));
        }
    }

    public void merge() throws Exception {
        createInputMergers(inputFiles);
        createOutputMerger(output);
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.info("Waiting for threads to finish");
        }
    }

    public boolean isAllInputsRead() {
        return numInputMergersFinished.get() == NUM_THREADS;
    }




    /* ************* Input *****************/

    void createInputMergers(List<File> inputFiles) throws Exception {
        final List<ThrowingSupplier<FileLine>> suppliers = new ArrayList<>(inputFiles.size());
        final int numInputs = inputFiles.size();
        for (File inputFile : inputFiles) {
            suppliers.add(new FileSupplier(inputFile));
        }

        final int[] firstIndex = new int[1];
        firstIndex[0] = 0;
        for (int segmentIndex = 0; segmentIndex < NUM_THREADS; segmentIndex++) {
            final int nextFirstIndex;
            if (segmentIndex == NUM_THREADS - 1) {
                nextFirstIndex = numInputs;
            } else {
                nextFirstIndex = (int) (((float) segmentIndex + 1) * numInputs / NUM_THREADS);
            }
            createInputMerger(suppliers.subList(firstIndex[0], nextFirstIndex), segmentIndex);
            firstIndex[0] = nextFirstIndex;
        } //for

    }

    private final AtomicLong linesGenerates = new AtomicLong(0);

    void createInputMerger(final List<ThrowingSupplier<FileLine>> suppliersSegment, final int segmentIndex) {
        executorService.submit(() -> {
            try {
                try (Merger merger = new Merger(suppliersSegment,
                        (line) -> {
                            queues.get(segmentIndex).put(line);
                            linesGenerates.addAndGet(1);
                        },
                        executorService)) {
                    merger.merge();
                }
                for (ThrowingSupplier<FileLine> supplier : suppliersSegment) {
                    ((AutoCloseable) supplier).close();
                }
                logger.info(linesGenerates.get() + " lines pushed to queues");
                numInputMergersFinished.addAndGet(1);
            } catch (Exception e) {
                logger.error("In input merger for segment with id " + segmentIndex + ". Suppliers" + suppliersSegment);
            }
        });
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

    /************** Output *****************/

    private void createOutputMerger(File output) throws Exception {
        final List<ThrowingSupplier<FileLine>> suppliers = new ArrayList<>(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            final int segmentIndex = i;
            suppliers.add(() -> {
                        final ArrayBlockingQueue<FileLine> queue = queues.get(segmentIndex);
                        while (!(isAllInputsRead() && queue.isEmpty())) {
                            FileLine line = queue.poll(10, TimeUnit.MILLISECONDS);
                            if (line != null) {
                                return line;
                            }
                        }
                        return null;
                    }
            ); //lambda
        } //for
        try (FileConsumer consumer = new FileConsumer(output);
             Merger merger = new Merger(suppliers, consumer, executorService)) {
            merger.merge();
        }
        logger.info("finished");
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


}
