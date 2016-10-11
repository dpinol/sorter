package org.dpinol;

import org.dpinol.log.Log;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Sorts a text file, line by line.
 * This class splits the line, and distributes them to {@link ChunkSorter}'s,
 * each of which will create several sorted files, which {@link ParallelFilesMerger} will merge
 * on a single file
 * File 5G (5M lines)
 * Mon Oct 10 01:56:58 CEST 2016: *** RUNNING WITH 5 threads, buffer size 10240, 10000 lines per sorter, 5 buckets of size 10000
 * 26s map
 * 4'20s reduce
 * total 4m47.552s
 */
public class BigFileSorter {
    private final static Log logger = new Log(BigFileSorter.class);

    /** Maximum number of simultaneous parallel cores to use in the whole application (includes main thread)*/
    private static final int MAX_NUM_THREADS = 6; //6-> 11.8, 5 ->11.3, 4->11.8, 2->11.2
    static final int LINES_PER_SORTER = 10_000;
    private static final int NUM_SORTERS = MAX_NUM_THREADS - 1;
    static final int QUEUE_BUCKET_SIZE = 1_000;
    static final int QUEUE_NUM_BUCKETS = MAX_NUM_THREADS;

    private final File input;
    private final File output;
    private final File tmpFolder;
    private final List<File> tmpFiles = new ArrayList<>(NUM_SORTERS);
    private final List<ChunkSorter> sorters = new ArrayList<>(NUM_SORTERS);
    private final ArrayBlockingQueue<LineBucket> queue = new ArrayBlockingQueue<>(QUEUE_NUM_BUCKETS);


    /**
     * @param input     must exist and not be empty
     * @param tmpFolder if null, it will be written to output folder
     */
    BigFileSorter(File input, File output, File tmpFolder) throws IOException {
        if (!input.isFile() || input.length() == 0) {
            throw new IllegalArgumentException("Input file " + input + " non existent or empty");
        }
        if (QUEUE_BUCKET_SIZE > LINES_PER_SORTER)
            throw new AssertionError("QUEUE_BUCKET_SIZE > LINES_PER_SORTER");
        logger.info("*** RUNNING WITH " + MAX_NUM_THREADS + " threads, "
                + "buffer size " + Global.BUFFER_SIZE + ", "
                + LINES_PER_SORTER + " lines per sorter, "
                + QUEUE_NUM_BUCKETS + " buckets of size " + QUEUE_BUCKET_SIZE);
        this.input = input;
        this.output = output;
        if (tmpFolder == null) {
            File parent = output.getParentFile();
            if (parent == null) parent = new File(".");
            Path tmpParent = Paths.get(parent.getAbsolutePath());
            this.tmpFolder = Files.createTempDirectory(tmpParent, "sorter", new FileAttribute[0]).toFile();
        } else {
            this.tmpFolder = tmpFolder;
        }
        logger.info("Using temporary folder at %s", this.tmpFolder);
    }


    void sort() throws Exception {
        map();
        reduce();
    }


    /**
     * Reads the input file lines sequentially, and pushes buckets of lines into a queue so that
     * {@link ChunkSorter}'s running in parallel sort them in separate files
     */
    private void map() throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(MAX_NUM_THREADS - 1);
        for (int i = 0; i < BigFileSorter.NUM_SORTERS; i++) {
            sorters.add(new ChunkSorter(this.tmpFolder, Integer.toString(i), executorService, queue));
        }

        long bytesRead = 0;
        long lastBytesLog = 0;
        try (FileLineReader fileLineReader = new FileLineReader(input)) {
            FileLine fileLine;
            LineBucket bucket = new LineBucket();
            while ((fileLine = fileLineReader.readFileLine()) != null) {
                bytesRead += fileLine.getNumBytes();
                if (bytesRead - lastBytesLog > 100 * 1_024 * 1_204) {
                    logger.info("Read " + bytesRead / 1_024 + "kB");
                    lastBytesLog = bytesRead;
                }
                bucket.add(fileLine);
                if (bucket.isFull()) {
                    queue.put(bucket);
                    bucket = new LineBucket();
                }
            }
            if (!bucket.isEmpty()) {
                queue.put(bucket);
            }
            //must close before closing the reader, because they'll close the input file handle
            closeSorters(executorService);
        }
    }

    private void closeSorters(ExecutorService executorService) throws IOException, InterruptedException {
        closeExecutor(executorService);
        for (ChunkSorter sorter : sorters) {
            sorter.close();
            tmpFiles.addAll(sorter.getFiles());
        }
    }

    void closeExecutor(ExecutorService executorService) throws InterruptedException {
        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.info("Waiting for flushers");
        }

    }

    private void reduce() throws Exception {
        List<File> noEmpties = tmpFiles.stream().filter((f) -> f.length() != 0).collect(Collectors.toList());
        if (noEmpties.size() == 1) {
            noEmpties.get(0).renameTo(output);
        } else {
            try (ParallelFilesMerger filesMerger = new ParallelFilesMerger(noEmpties, output, MAX_NUM_THREADS)) {
                filesMerger.merge();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        File tmpFolder = null;
        if (args.length < 2) {
            System.err.println("Usage: " + BigFileSorter.class.getName() + " inputFile outputFile [tmpFolder]");
            System.err.println("If tmpFolder not provided, a tmp folder will be created and used within outputFile");
            System.exit(-1);
        } else if (args.length == 3 && !args[2].isEmpty()) {
            tmpFolder = new File(args[2]);
        }
        BigFileSorter bigFileSorter = new BigFileSorter(new File(args[0]), new File(args[1]), tmpFolder);
        bigFileSorter.sort();
    }
}