package com.company;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.company.Global.BUFFER_SIZE;
import static com.company.Global.LINE_SEPARATOR_BYTES;

/**
 * Created by dani on 23/09/16.
 */
public class BigLineReader implements AutoCloseable {
    private final Path input;
    private final FileChannel fileChannel;
    private final ByteBuffer buffer;
    private long lineStartFileOffset = 0;
    private int currentBufferOffset = 0;
    private int bufferSize = 0;


    public BigLineReader(File input) throws IOException {
        this(Paths.get(input.getAbsolutePath()));
    }

    public BigLineReader(Path input) throws IOException {
        fileChannel = FileChannel.open(input);
        this.input = input;
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        //set to end so that we're forced to read from file initially
//        currentBufferOffset = BUFFER_SIZE;
    }


    //reading 800Mb of short lines takes 8s instead of 3s if using 10 * 1024
    final ByteArrayOutputStream lineHead = new ByteArrayOutputStream(BUFFER_SIZE);

    /**
     * @return null when EOF found
     */
    BigLine getBigLine() throws IOException {
        long lineLength = 0;
        int nlPos;
        lineHead.reset();
        do {
            if (currentBufferOffset >= bufferSize) {
                buffer.clear();
                currentBufferOffset = 0;
                bufferSize = fileChannel.read(buffer);
                buffer.flip();
                if (bufferSize < 0) {
                    break;
                }
            }
            nlPos = findNewLine(buffer, currentBufferOffset, bufferSize);
            int newChunkLen;
            if (nlPos < 0)
                newChunkLen = bufferSize - currentBufferOffset;
            else
                newChunkLen = nlPos - currentBufferOffset;
            lineLength += newChunkLen;
            if (lineHead.size() < BUFFER_SIZE) {
                int bytesToCopy = Math.min(BUFFER_SIZE, newChunkLen);
                bytesToCopy = Math.min(bytesToCopy, BUFFER_SIZE - lineHead.size());
                lineHead.write(buffer.array(), currentBufferOffset, bytesToCopy);
            }
            currentBufferOffset += newChunkLen + 1;
        } while (nlPos < 0);
        if (lineHead.size() == 0) {
            return null;
        }
        long curStartOffset = lineStartFileOffset;
        lineStartFileOffset += lineLength + 1;
        if (lineLength <= BUFFER_SIZE)
            return new ShortLine(lineHead.toString());
        else
            return new LongLine(fileChannel, lineHead.toString(), curStartOffset, lineLength);
    }

    /**
     * Find the first new line in a buffer
     *
     * @return -1 if not found
     */
    private static int findNewLine(ByteBuffer aBuffer, int startIndex, long bufSize) {
        byte[] array = aBuffer.array();
        //TODO is there an efficient char search?
        for (int i = startIndex; i < bufSize; i++) {
            if (arrayContains(array, i, LINE_SEPARATOR_BYTES))
                return i;
        }
        return -1;
    }

    private static boolean arrayContains(byte[] haystack, int offset, byte[] needle) {
        for (int i = 0; i < needle.length; i++) {
            if (haystack[i + offset] != needle[i])
                return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
