package com.company;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static com.company.Global.BUFFER_SIZE;
import static com.company.Global.log;

/**
 * Created by dani on 22/09/16.
 */
public abstract class BigLine implements Comparable<BigLine> {

//    static BigLine create(String line) {
//        return new LongLine(line);
//    }

    public abstract long getNumBytes();


    @Override
    public int compareTo(BigLine o) {
        try {
            int startIndex = 0;
            int endIndex = BUFFER_SIZE;
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
                } else if (l1.length() != BUFFER_SIZE) {
                    return 0;
                }
                startIndex += BUFFER_SIZE;
                endIndex += BUFFER_SIZE;
            } while (true);
        } catch (IOException e) {
            throw new RuntimeException("comparing " + this + " to " + o, e);
        }
    }

    abstract public Iterator<String> getIterator() throws IOException;


    public void write(Writer writer) throws IOException {
        Iterator<String> iterator = getIterator();
        while (iterator.hasNext()) {
            writer.write(iterator.next());
        }
    }
}

class ShortLine extends BigLine {
    String line;

    public ShortLine(String line) {
        this.line = line;
    }

    @Override
    public long getNumBytes() {
        return line.getBytes().length;
    }

    @Override
    public int compareTo(BigLine o) {
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


class LongLine extends BigLine {
    /* we cache first buffer so that most of times we don't need to hit the disk for comparing with other lines*/
    private final String head;
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final long startFileOffset;
    private final long numBytes;
    private final FileChannel fileChannel;


    /**
     * @param fileChannel     LongLine will not query nor change its current position
     */
    public LongLine(FileChannel fileChannel, String lineHead, long startFileOffset, long numBytes) throws IOException {
        log("LONG LINE************");
        this.fileChannel = fileChannel;
        head = lineHead;
        this.startFileOffset = startFileOffset;
        this.numBytes = numBytes;
        this.buffer.limit(BUFFER_SIZE);
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
                        if (getRelativeCurrentOffset() + BUFFER_SIZE > numBytes) {
                            buffer.limit((int) (numBytes - getRelativeCurrentOffset()));
                        }
                        buffer.clear();
                        fileChannel.read(buffer, currentOffset);
                        currentOffset += BUFFER_SIZE;
                        ret = new String(buffer.array(), 0, buffer.limit());
                    } catch (IOException e) {
                        throw new RuntimeException("reading file", e);
                    }
                }
                if (currentOffset >= startFileOffset + numBytes)
                    hasNext = false;
                return ret;
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