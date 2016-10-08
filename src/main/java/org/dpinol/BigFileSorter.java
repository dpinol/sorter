package org.dpinol;

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
 * each of which will create several sorted files, which {@link Merger} will merge
 * on a single file
 */
public class BigFileSorter {

    //TODO with threaded sort, 100_000 is much slower than 10_000
    static final int LINES_PER_SORTER = 10_000;
    private static final int NUM_SORTERS = 5; //6-> 11.8, 5 ->11.3, 4->11.8, 2->11.2
    private static final int NUM_THREADS = NUM_SORTERS;
    static final int QUEUE_BUCKET_SIZE = 10_000;
    static final int QUEUE_NUM_BUCKETS = NUM_THREADS ;

    private static final Random rnd = new Random();

    private final File input;
    private final File output;
    private final File tmpFolder;
    private final List<File> tmpFiles = new ArrayList<>(NUM_SORTERS);
    private List<ChunkSorter> sorters = new ArrayList<>(NUM_SORTERS);
    //with newWorkStealingPool I get RejectedExecutionException
    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    private ArrayBlockingQueue<LineBucket> queue = new ArrayBlockingQueue<>(QUEUE_NUM_BUCKETS);


    /**
     * @param tmpFolder if null, it will be written to output folder
     */
    BigFileSorter(File input, File output, File tmpFolder) throws IOException {
        if (QUEUE_BUCKET_SIZE > LINES_PER_SORTER)
            throw new AssertionError("QUEUE_BUCKET_SIZE > LINES_PER_SORTER");
        Global.log("*** RUNNING WITH " + NUM_THREADS + " threads, "
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
        for (int i = 0; i < BigFileSorter.NUM_SORTERS; i++) {
            sorters.add(new ChunkSorter(this.tmpFolder, Integer.toString(i), executorService, queue));
        }
    }


    void sort() throws Exception {
        map();
        reduce();
    }


    private void map() throws Exception {
        long bytesRead = 0;
        long lastBytesLog = 0;
        try (FileLineReader fileLineReader = new FileLineReader(input)) {
            FileLine fileLine;
            LineBucket bucket = new LineBucket();
            while ((fileLine = fileLineReader.getBigLine()) != null) {
                bytesRead += fileLine.getNumBytes();
                if (bytesRead - lastBytesLog > 100 * 1_024 * 1_204) {
                    Global.log("Read " + bytesRead / 1_024 + "kB");
                    lastBytesLog = bytesRead;
                }
//                ChunkSorter chunkSorter = sorters.get(rnd.nextInt(NUM_SORTERS));
//                chunkSorter.addLine(fileLine);
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
            closeSorters();
        }
    }

    private void closeSorters() throws IOException, InterruptedException {
//        for (ChunkSorter sorter : sorters) {
//            sorter.close();
//            tmpFiles.addAll(sorter.getFiles());
//        }
        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            Global.log("Waiting for flushers");
        }

        for (ChunkSorter sorter : sorters) {
            sorter.close();
            tmpFiles.addAll(sorter.getFiles());
        }
    }

    private void reduce() throws Exception {
        try (Merger merger = new Merger(tmpFiles, output)) {
            merger.merge();
        }
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