package org.dpinol;

import org.dpinol.data.SimpleHeap;
import org.dpinol.log.Log;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * They get lines which are stored on a heap, and when it has more than {@link BigFileSorter#LINES_PER_SORTER} lines
 * they are flushed. The flush is done on a different thread
 */
class ChunkSorter implements AutoCloseable {
    private final static Log logger = new Log(ChunkSorter.class);

    private final File tmpFolder;
    private final ArrayBlockingQueue<LineBucket> queue;
    private final String id;
    private final ExecutorService executorService;
    private SimpleHeap<FileLine> heap = new SimpleHeap<>(BigFileSorter.LINES_PER_SORTER);
    private final List<File> files = new ArrayList<>();
    private final SorterThread sorterThread;
    private int waitCounter = 0;

    ChunkSorter(File tmpFolder, String id, ExecutorService executorService,
                ArrayBlockingQueue<LineBucket> queue) throws IOException {
        this.tmpFolder = tmpFolder;
        this.queue = queue;
        tmpFolder.deleteOnExit();
        this.id = id;
        this.executorService = executorService;
        sorterThread = new SorterThread();
        executorService.submit(sorterThread);
    }

    public List<File> getFiles() {
        return files;
    }


    @Override
    public void close() throws IOException, InterruptedException {
    }

    private class SorterThread extends Thread {
        private File tmpFile;

        @Override
        public void run() {

            while (!isDone()) {
                try {
                    fillHeap();
                    if (!heap.isEmpty()) {
                        tmpFile = File.createTempFile("sort_tmp", id, tmpFolder);
                        tmpFile.deleteOnExit();
                        files.add(tmpFile);
                        flush();
                    }
                } catch (Exception e) {
                    System.err.println("Error writing ChunkSorter to " + tmpFile + ": " + e);
                    e.printStackTrace();
                }
            }
            logger.debug("Finished %s after %d waits", sorterThread, waitCounter);
        }

        boolean isDone() {
            return queue.isEmpty() && executorService.isShutdown();
        }

        void fillHeap() throws InterruptedException {
            while (heap.size() < BigFileSorter.LINES_PER_SORTER && !isDone()) {
                LineBucket bucket = queue.poll(10, TimeUnit.MILLISECONDS);
                if (bucket != null) {
                    for (FileLine fileLine : bucket) {
                        heap.add(fileLine);
                    }
                } else {
                    waitCounter++;
                }
            }
        }


        void flush() throws Exception {
//            logger.info("Flushing file " + tmpFile);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile))) {
                FileLine line;
                while ((line = heap.poll()) != null) {
                    line.write(writer);
                }
            }

        }
    }


}
