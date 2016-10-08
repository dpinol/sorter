package com.company;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BigFileSorter {
    static final int READ_BUF_SIZE = 1024 * 1024;
    static final int LINES_PER_SORTER = 100_000;
    static final int NUM_SORTERS = 2;
    static final int NUM_THREADS= NUM_SORTERS;
    static final Random rnd = new Random();

    private final File input;
    private final File output;
    private final File tmpFolder;
    private final List<File> tmpFiles = new ArrayList<>(NUM_SORTERS);
    List<ChunkSorter> sorters = new ArrayList<>(NUM_SORTERS);
    //with newWorkStealingPool I get RejectedExecutionException
    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);


    /**
     * @param input
     * @param output
     * @param tmpFolder if null, it will be written to output folder
     * @throws IOException
     */
    BigFileSorter(File input, File output, File tmpFolder) throws IOException {
        Global.log("********************** RUNNING WITH " +  NUM_THREADS + " threads **********************");
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
            sorters.add(new ChunkSorter(this.tmpFolder, Integer.toString(i), executorService));
        }
    }


    void sort() throws Exception {
        map();
        reduce();
    }


    private void map() throws Exception {
        long bytesRead = 0;
        long lastBytesLog = 0;
        try (BigLineReader bigLineReader = new BigLineReader(input)) {
            BigLine bigLine;
            while ((bigLine = bigLineReader.getBigLine()) != null) {
                bytesRead += bigLine.getNumBytes();
                if (bytesRead - lastBytesLog > 100 * 1_024 * 1_204) {
                    Global.log("Read " + bytesRead / 1_024 + "kB");
                    lastBytesLog = bytesRead;
                }
                ChunkSorter chunkSorter = sorters.get(rnd.nextInt(NUM_SORTERS));
                File newFile = chunkSorter.addLine(bigLine);
                if (newFile != null) {
                    tmpFiles.add(newFile);
                }
            }
            //must close before closing the reader, because they'll close the input file handle
            closeSorters();
        }
    }

    private void closeSorters() throws IOException, InterruptedException {
        for (ChunkSorter sorter : sorters) {
            sorter.close();
        }
        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            Global.log("Waiting for flushers");
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