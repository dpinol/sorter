package org.dpinol;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by dani on 05/10/2016.
 */
public class PerformanceTest {
    static final int NUM_THREADS = 16;
    static final int NUM_LINES = 5_000_000;
    static final int QUEUE_BUCKET_SIZE = 1000;
    static final int QUEUE_NUM_BUCKETS = 1000;

    static class LineBucket extends ArrayList<FileLine> {
        LineBucket() {
            super(QUEUE_BUCKET_SIZE);
        }

        boolean isFull() {
            return size() == QUEUE_BUCKET_SIZE;
        }
    }

    private ArrayBlockingQueue<LineBucket> queue = new ArrayBlockingQueue<>(QUEUE_NUM_BUCKETS);
    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    private static final Random rnd = new Random();

    @Test
    public void queue() throws Exception {
        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.submit(new Consumer());
        }

        try (Timer timer = new Timer()) {
            LineBucket bucket = new LineBucket();
            for (int i = 0; i < NUM_LINES; i++) {
                bucket.add(new ShortLine("line" + rnd.nextInt()));
                if (i % 1_000_000 == 0) {
                    Global.log("Bucket pushed " + i);
                }
                if (bucket.isFull()) {
                    queue.put(bucket);
                    bucket = new LineBucket();
                }
            }
            if (!bucket.isEmpty()) {
                queue.put(bucket);
            }
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    class Consumer implements Runnable {
        int hc = 0;
        int waits = 0;

        @Override
        public void run() {
            SimpleHeap<FileLine> heap = new SimpleHeap<>(100_000);
            try {
                while (!executorService.isShutdown() || !queue.isEmpty()) {
                    LineBucket bucket = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (bucket == null) {
                        waits++;
                        continue;
                    }

                    for (FileLine fileLine : bucket) {
                        consumeLine(heap, fileLine);
                    }
                }
                Global.log("lines read " + hc + ". Waits " + waits);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void consumeLine(SimpleHeap<FileLine> heap, FileLine line) {
//                        if (hc % 10 == 0) {
//                            Thread.sleep(1);
//                        }
            if (heap.isFull()) {
                while (!heap.isEmpty()) {
                    heap.poll();
                }
            }
            heap.add(line);
            hc++;

        }
    }
}
