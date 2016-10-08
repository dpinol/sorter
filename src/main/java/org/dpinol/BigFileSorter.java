package org.dpinol;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Sorts a text file, line by line.
 * This class splits the line, and distributes them to {@link ChunkSorter}'s,
 * each of which will create several sorted files, which {@link Merger} will merge
 * on a single file
 * <p>
 * 5G file:
 * parallelizing sorting in map
 * 5 threads: 30s map and 4m40s
 * 2 threads: 35s map
 * 8 threads: 33s
 * with old 2 threads: 37s
 * with 5 threads 52s
 */
public class BigFileSorter {

    //TODO with threaded sort, 100_000 is much slower than 10_000
    static final int LINES_PER_SORTER = 200_000;
    static final int NUM_SORTERS = 256; //6-> 11.8, 5 ->11.3, 4->11.8, 2->11.2
    private static final int NUM_THREADS = NUM_SORTERS;
    static final int SORTER_QUEUE_SIZE = 1_0;

    private static final Random rnd = new Random();

    private final File input;
    private final File output;
    private final File tmpFolder;
    private final Distributer distributer = new Distributer();
//    private final List<File> tmpFiles = new ArrayList<>(NUM_SORTERS);
    private final ChunkSorter[] sorters = new ChunkSorter[distributer.getMaximumIndices()];
    //with newWorkStealingPool I get RejectedExecutionException
    private final ExecutorService executorService = Executors.newWorkStealingPool(NUM_SORTERS);


    /**
     * @param tmpFolder if null, it will be written to output folder
     */
    BigFileSorter(File input, File output, File tmpFolder) throws IOException {
        Global.log("*** RUNNING WITH " + NUM_THREADS + " threads, "
                + LINES_PER_SORTER + " lines per sorter, "
                + SORTER_QUEUE_SIZE + " lines in each sorter queue");
        this.input = input;
        this.output = output;
        if (tmpFolder == null) {
            File parent = output.getParentFile();
            if (parent == null) parent = new File(".");
            Path tmpParent = Paths.get(parent.getAbsolutePath());
            this.tmpFolder = Files.createTempDirectory(tmpParent, "sorter", new FileAttribute[0]).toFile();
            this.tmpFolder.deleteOnExit();
        } else {
            this.tmpFolder = tmpFolder;
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
            while ((fileLine = fileLineReader.getBigLine()) != null) {
                bytesRead += fileLine.getNumBytes();
                if (bytesRead - lastBytesLog > 50 * 1_024 * 1_204) {
                    Global.log("Read " + bytesRead / 1_024 + "kB");
                    lastBytesLog = bytesRead;
                }
                int sorterIndex = distributer.getSorterIndex(fileLine);
                ChunkSorter chunkSorter = sorters[sorterIndex];
                if (chunkSorter == null) {
                    chunkSorter = new ChunkSorter(this.tmpFolder, Integer.toString(sorterIndex), executorService);
                    sorters[sorterIndex] = chunkSorter;
                }
                chunkSorter.addLine(fileLine);
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
            if (sorter != null) {
                sorter.close();
//                tmpFiles.addAll(sorter.getFiles());
            }
        }
    }

    private void reduce() throws Exception {
//        Merger.mergeFiles(tmpFiles, output);
        output.delete();
        Global.log("Final merge start");
        int numFullSorters = 0;
        try (FileChannel outputFC = FileChannel.open(Paths.get(output.toString()), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            for (ChunkSorter sorter : sorters) {
                if (sorter == null)
                    continue;
                numFullSorters++;
                Path inputPath = Paths.get(sorter.getMergedFile().toString());
                try (FileChannel inputFC = FileChannel.open(inputPath, StandardOpenOption.READ)) {
                    inputFC.transferTo(0, inputFC.size(), outputFC);
                }
            }
        }
        Global.log("Final merge end of " + numFullSorters + " files");
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