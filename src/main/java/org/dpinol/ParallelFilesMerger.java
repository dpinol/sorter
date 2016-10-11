package org.dpinol;

import org.dpinol.log.Log;
import org.dpinol.function.ThrowingConsumer;
import org.dpinol.function.ThrowingSupplier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Merges in parallel a list of sorted files into a single one.
 * - The files are split into {@link #MAX_NUM_MERGERS} segments of approx the same number of files.
 * - Each files segment is merged by an "input Merger", which pushes the sorted lines into a queue
 * - The main thread runs an "outpu tMerger", which reads from both queues and merges into the output file
 */
public class ParallelFilesMerger implements AutoCloseable {
    private final static Log logger = new Log(ParallelFilesMerger.class);

    private final static int MAX_NUM_MERGERS = 3;
    private final static int QUEUE_LEN = 1000;
    private final List<File> inputFiles;
    private final File output;
    private final ExecutorService executorService;
    private final List<ArrayBlockingQueue<FileLine>> queues;
    private final AtomicLong numInputMergersFinished = new AtomicLong(0);
    private final int numSegments;


    /**
     * @param inputFiles each input files must contain at least 1 line. Cannot be empty
     * @param maxNumThreads max number of cores to use including main one
     */
    public ParallelFilesMerger(List<File> inputFiles, File output, int maxNumThreads) throws Exception {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("no inputFiles passed");
        }
        numSegments = Math.min(MAX_NUM_MERGERS, inputFiles.size());
        queues = new ArrayList<>(numSegments);
        this.inputFiles = inputFiles;
        this.output = output;
        this.executorService = Executors.newFixedThreadPool(maxNumThreads);
        for (File inputFile : this.inputFiles) {
            if (inputFile.length() == 0) {
                throw new IllegalArgumentException("Input file "  +inputFile + " is empty");
            }
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
        return numInputMergersFinished.get() == numSegments;
    }




    /* ************* Input *****************/

    private void createInputMergers(List<File> inputFiles) throws Exception {
        final List<ThrowingSupplier<FileLine>> suppliers = new ArrayList<>(inputFiles.size());
        final int numInputs = inputFiles.size();
        for (File inputFile : inputFiles) {
            suppliers.add(new FileSupplier(inputFile));
        }

        final int[] firstIndex = new int[1];
        firstIndex[0] = 0;
        for (int segmentIndex = 0; segmentIndex < numSegments; segmentIndex++) {
            final int nextFirstIndex;
            if (segmentIndex == suppliers.size() - 1) {
                nextFirstIndex = numInputs;
            } else {
                nextFirstIndex = (int) (((float) segmentIndex + 1) * numInputs / numSegments);
            }
            createInputMerger(suppliers.subList(firstIndex[0], nextFirstIndex), segmentIndex);
            firstIndex[0] = nextFirstIndex;
        }
    }

    private final AtomicLong linesGenerates = new AtomicLong(0);

    private void createInputMerger(final List<ThrowingSupplier<FileLine>> suppliersSegment, final int segmentIndex) {
        executorService.submit(() -> {
            try {
                try (Merger inputMerger = new Merger(suppliersSegment,
                        (line) -> {
                            queues.get(segmentIndex).put(line);
                            linesGenerates.addAndGet(1);
                        },
                        executorService)) {
                    inputMerger.merge();
                }
                for (ThrowingSupplier<FileLine> supplier : suppliersSegment) {
                    ((AutoCloseable) supplier).close();
                }
                logger.info(linesGenerates.get() + " lines pushed to queues");
                numInputMergersFinished.addAndGet(1);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("In input merger for segment with id %d of length %d", segmentIndex, suppliersSegment.size());
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
        final List<ThrowingSupplier<FileLine>> suppliers = new ArrayList<>(numSegments);
        for (int i = 0; i < numSegments; i++) {
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
