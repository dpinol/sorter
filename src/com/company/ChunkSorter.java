package com.company;

import java.io.*;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RecursiveAction;

/**
 * They get lines which are stored on a heap, and when it has more than {@link BigFileSorter#LINES_PER_SORTER} lines
 * they are flushed. The flush is done on a different thread
 * Created by dani on 20/09/16.
 */
class ChunkSorter implements AutoCloseable {
    private final File tmpFolder;
    private File nextTmpFile;
    private final String id;
    private final ExecutorService executorService;
    private PriorityQueue<BigLine> heap = new PriorityQueue<>(BigFileSorter.LINES_PER_SORTER);


    ChunkSorter(File tmpFolder, String id, ExecutorService executorService) throws IOException {
        this.tmpFolder = tmpFolder;
        tmpFolder.deleteOnExit();
        this.id = id;
        this.executorService = executorService;
        createTmpFile();
    }

    /**
     * If a new temporary file is created, it's returned. Otherwise, null
     */
    public File addLine(BigLine line) throws IOException {
        if (heap.size() >= BigFileSorter.LINES_PER_SORTER) {
            flush();
            createTmpFile();
        }
        heap.add(line);
        if (heap.size() == 1) {
            return nextTmpFile;
        } else {
            return null;
        }
    }

    private void createTmpFile() throws IOException {
        nextTmpFile = File.createTempFile("sort_tmp", id, tmpFolder);
        nextTmpFile.deleteOnExit();
    }

    private void flush() throws IOException {
        PriorityQueue<BigLine> newHeap = new PriorityQueue<>(BigFileSorter.LINES_PER_SORTER);
        executorService.submit(new Flusher(heap, nextTmpFile));
        heap = newHeap;
    }

    @Override
    public void close() throws IOException {
        if (!heap.isEmpty()) {
            flush();
        }
    }

    private static class Flusher implements Runnable {
        private final PriorityQueue<BigLine> heapToFlush;
        private final File tmpFile;

        Flusher(PriorityQueue<BigLine> heapToFlush, File tmpFile) {
            this.heapToFlush = heapToFlush;
            this.tmpFile = tmpFile;
        }

        @Override
        public void run() {
            try {
                //Global.log("Flushing file " + tmpFile);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile))) {
                    BigLine line;
                    while ((line = heapToFlush.poll()) != null) {
                        line.write(writer);
                        writer.newLine();
                    }
                }
            } catch (Exception e) {
                System.err.println("Error writing ChunkSorter to " + tmpFile + ": " + e);
            }
        }

    }


    class Action extends RecursiveAction {

        @Override
        protected void compute() {

        }
    }


}
