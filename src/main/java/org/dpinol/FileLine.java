package org.dpinol;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * It gives access to a line parsed from a file. If shorter than {@link #LENGTH_THRESHOLD},
 * the whole text is held in a {@link ShortLine}. Otherwise, a {@link LongLine} holds the
 * position of the text within the file.
 */
public abstract class FileLine implements Comparable<FileLine> {

    static final int LENGTH_THRESHOLD = org.dpinol.Global.BUFFER_SIZE;

    public abstract long getNumBytes();


    @Override
    public int compareTo(FileLine o) {
        try {
            int comp;
            Iterator<String> i1 = getIterator();
            Iterator<String> i2 = o.getIterator();
            do {
                if (!i1.hasNext() && !i2.hasNext()) {
                    return 0;
                } else if (!i1.hasNext() && i2.hasNext()) {
                    return -1;
                } else if (i1.hasNext() && !i2.hasNext()) {
                    return 1;
                }
                String l1 = i1.next();
                String l2 = i2.next();
                comp = l1.compareTo(l2);
                if (comp != 0) {
                    return comp;
                }
            } while (true);
        } catch (IOException e) {
            throw new RuntimeException("comparing " + this + " to " + o, e);
        }
    }

    /**
     * TODO return Iterator<Future<String>> so that 2 LongLine's being compared can be read in parallel
     *
     * @return an iterator to access the line in chunks of maximum {@link #LENGTH_THRESHOLD}
     */
    abstract public Iterator<String> getIterator() throws IOException;


    /**
     * Writes the line with a final new Line
     *
     */
    public CompletableFuture<Void> writeAsync(Writer writer, ExecutorService executorService) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        Iterator<String> iterator = getIterator();
                        while (iterator.hasNext()) {
                            writer.write(iterator.next());
                        }
                        writer.write(Global.LINE_SEPARATOR);
                        cf.complete(null);
                    } catch (IOException e) {
                        cf.completeExceptionally(e);
                    }
                }, executorService);
        return cf;
    }

    public void write(Writer writer) throws IOException {
        Iterator<String> iterator = getIterator();
        while (iterator.hasNext()) {
            writer.write(iterator.next());
        }
        writer.write(Global.LINE_SEPARATOR);
    }

}

/**
 * Whole line is held in memory
 */
class ShortLine extends FileLine {
    private String line;

    public ShortLine(String line) {
        this.line = line;
    }

    @Override
    public long getNumBytes() {
        return line.getBytes().length;
    }

    @Override
    public int compareTo(FileLine o) {
        if (o instanceof ShortLine) {
            return line.compareTo(((ShortLine) o).line);
        }
        return 0;
    }


    @Override
    public Iterator<String> getIterator() {
        return new Iterator<String>() {
            boolean consumed = false;

            @Override
            public boolean hasNext() {
                return !consumed;
            }

            @Override
            public String next() {
                consumed = true;
                return line;
            }
        };
    }

} //ShortLine

/**
 * Holds the information to quickly read a text line from a file
 */
class LongLine extends FileLine {
    /* we cache first buffer so that most of times we don't need to hit the disk for comparing with other lines*/
    private final String head;
    private final ByteBuffer buffer = ByteBuffer.allocate(LENGTH_THRESHOLD);
    /**
     * Offset of the line within the file
     */
    private final long startFileOffset;
    /**
     * Length in bytes of the line
     */
    private final long numBytes;
    private final AsynchronousFileChannel asynchronousFileChannel;


    /**
     * @param asynchronousFileChannel LongLine will not query nor change its current position. It cannot be closed until the LongLine
     *                                finishes reading the file
     */
    public LongLine(AsynchronousFileChannel asynchronousFileChannel, String lineHead, long startFileOffset, long numBytes) throws IOException {
        this.asynchronousFileChannel = asynchronousFileChannel;
        head = lineHead;
        this.startFileOffset = startFileOffset;
        this.numBytes = numBytes;
        this.buffer.limit(LENGTH_THRESHOLD);
    }

    @Override
    public Iterator<String> getIterator() throws IOException {
        return new Iterator<String>() {
            long currentOffset = startFileOffset;
            boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            long getRelativeCurrentOffset() {
                return currentOffset - startFileOffset;
            }

            @Override
            public String next() {
                String ret;
                if (currentOffset == startFileOffset) {
                    currentOffset += head.length();
                    ret = head;
                } else {
                    try {
                        buffer.clear();
                        int readBytes = read();
                        int chunkSize = Math.min(readBytes, (int) (numBytes - getRelativeCurrentOffset()));
                        ret = new String(buffer.array(), 0, chunkSize);
                        currentOffset += LENGTH_THRESHOLD;
                    } catch (Exception e) {
                        throw new RuntimeException("reading file", e);
                    }
                }
                if (currentOffset >= startFileOffset + numBytes)
                    hasNext = false;
                return ret;
            }

            int read() throws Exception {
                return asynchronousFileChannel.read(buffer, currentOffset).get();
            }
        };


    }


    /**
     * @return the full length of the line
     */
    @Override
    public long getNumBytes() {
        return numBytes;
    }

}