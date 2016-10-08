package org.dpinol;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * They get lines which are stored on a heap, and when it has more than {@link BigFileSorter#LINES_PER_SORTER} lines
 * they are flushed. The flush is done on a different thread
 * <p>
 * 5G file:
 * only parallelizing write
 * with old 2 threads: 37s
 * with 5 threads 52s
 * parallelizing sorting
 * 5 threads: 30s map and 4m40s
 * 2 threads: 35s map
 * 8 threads: 33s
 */
class ChunkSorter implements AutoCloseable {
    private final File tmpFolder;
    private final String id;
    private final ExecutorService executorService;
    private final SimpleHeap<FileLine> heap = new SimpleHeap<>(BigFileSorter.LINES_PER_SORTER);
    private final List<File> files = new ArrayList<>();
    private File mergedFile;
    private final Flusher flusher;
    private int waitCounter = 0;
    private final ArrayBlockingQueue<FileLine> queue = new ArrayBlockingQueue<>(BigFileSorter.SORTER_QUEUE_SIZE);


    ChunkSorter(File tmpFolder, String id, ExecutorService executorService) throws IOException {
        this.tmpFolder = tmpFolder;
        this.id = id;
        this.executorService = executorService;
        flusher = new Flusher();
        executorService.submit(flusher);
    }

    public File getMergedFile() {
        return mergedFile;
    }


    void addLine(FileLine line) throws InterruptedException {
        queue.put(line);
    }

    @Override
    public void close() throws IOException, InterruptedException {

        //flusher.shutDown = true;
//        flusher.join();
        if (waitCounter > 1000) {
            Global.log("joined " + flusher + " after " + waitCounter + " waits");
        }
        if (files.size() == 1) {
            mergedFile = files.get(0);
        } else {
            mergedFile = File.createTempFile("chunk_sorter_merged", id, tmpFolder);
            mergedFile.deleteOnExit();
            Merger.mergeFiles(files, mergedFile);
        }
    }

    void mergeOutputsFiles() {

    }

    private class Flusher extends Thread {
        private File tmpFile;
        //volatile boolean shutDown = false;

        @Override
        public void run() {

            while (!isDone()) {
                try {
                    fillHeap();
                    tmpFile = File.createTempFile("sort_tmp", id, tmpFolder);
                    tmpFile.deleteOnExit();
                    files.add(tmpFile);
                    flush();
                } catch (Exception e) {
                    System.err.println("Error writing ChunkSorter to " + tmpFile + ": " + e);
                    e.printStackTrace();
                }
            }
//            Global.log("done " + flusher);
        }

        boolean isDone() {
            return queue.isEmpty() && executorService.isShutdown();
        }

        void fillHeap() throws InterruptedException {
            while (heap.size() < BigFileSorter.LINES_PER_SORTER && !isDone()) {
                FileLine fileLine = queue.poll(10, TimeUnit.MILLISECONDS);
                if (fileLine != null) {
                    heap.add(fileLine);
                } else {
                    waitCounter++;
                }
            }
        }


        void flush() throws Exception {
//            Global.log("Flushing file " + tmpFile);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile))) {
                FileLine line;
                while ((line = heap.poll()) != null) {
                    line.write(writer);
                    writer.newLine();
                }
            }

        }
    }


}
