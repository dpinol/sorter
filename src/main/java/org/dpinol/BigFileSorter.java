package org.dpinol;

import org.dpinol.util.Log;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Sorts a text file, line by line.
 * This class splits the line, and distributes them to {@link ChunkSorter}'s,
 * each of which will create several sorted files, which {@link FilesMerger} will merge
 * on a single file
 *  File 5G (5M lines)
 *  Mon Oct 10 01:56:58 CEST 2016: *** RUNNING WITH 5 threads, buffer size 10240, 10000 lines per sorter, 5 buckets of size 10000
 * 26s map
 * 4'20s reduce
 * total 4m47.552s
 */
public class BigFileSorter {

    //TODO with threaded sort, 100_000 is much slower than 10_000
    static final int LINES_PER_SORTER = 10_000;
    private static final int NUM_SORTERS = 5; //6-> 11.8, 5 ->11.3, 4->11.8, 2->11.2
    private static final int NUM_THREADS = NUM_SORTERS;
    static final int QUEUE_BUCKET_SIZE = 1_000;
    static final int QUEUE_NUM_BUCKETS = NUM_THREADS;

    private static final Random rnd = new Random();

    private final File input;
    private final File output;
    private final File tmpFolder;
    private final List<File> tmpFiles = new ArrayList<>(NUM_SORTERS);
    private List<ChunkSorter> sorters = new ArrayList<>(NUM_SORTERS);
    private ArrayBlockingQueue<LineBucket> queue = new ArrayBlockingQueue<>(QUEUE_NUM_BUCKETS);


    /**
     * @param tmpFolder if null, it will be written to output folder
     */
    BigFileSorter(File input, File output, File tmpFolder) throws IOException {
        if (QUEUE_BUCKET_SIZE > LINES_PER_SORTER)
            throw new AssertionError("QUEUE_BUCKET_SIZE > LINES_PER_SORTER");
        Log.info("*** RUNNING WITH " + NUM_THREADS + " threads, "
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
    }


    void sort() throws Exception {
        map();
        reduce();
    }


    private void map() throws Exception {
        //with newWorkStealingPool I get RejectedExecutionException
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
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
                    Log.info("Read " + bytesRead / 1_024 + "kB");
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
            Log.info("Waiting for flushers");
        }

    }

    private void reduce() throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        try (FilesMerger filesMerger = new FilesMerger(tmpFiles, output, executorService)) {
            filesMerger.merge();
        }
        closeExecutor(executorService);
    }

    public static void main(String[] args) throws Exception {
        File tmpFolder = null;
        // write your code here
        if (args.length < 2) {
            System.err.println("Usage: " + BigFileSorter.class.getName() + " inputFile outputFile [tmpFolder]");
            System.err.println("If tmpFolder not provide, tmp files will be written in same folder as outputFile");
            System.exit(-1);
        } else if (args.length == 3) {
            tmpFolder = new File(args[2]);
        }
        BigFileSorter bigFileSorter = new BigFileSorter(new File(args[0]), new File(args[1]), tmpFolder);
        bigFileSorter.sort();
    }
}