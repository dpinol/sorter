//package org.dpinol;
//
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//
///**
// * Reads an input file, creating an instance if {@link FileLine} for parsed line
// */
//public class FileLineReader implements AutoCloseable {
//    private final Path input;
//    private final FileChannel fileChannel;
//    private final ByteBuffer buffer;
//    private long lineStartFileOffset = 0;
//    private int currentBufferOffset = 0;
//    private int bufferSize = 0;
//    final ByteArrayOutputStream lineHead = new ByteArrayOutputStream(Global.BUFFER_SIZE);
//
//
//    public FileLineReader(File input) throws IOException {
//        this(Paths.get(input.getAbsolutePath()));
//    }
//
//    public FileLineReader(Path input) throws IOException {
//        fileChannel = FileChannel.open(input);
//        this.input = input;
//        buffer = ByteBuffer.allocate(Global.BUFFER_SIZE);
//    }
//
//
//
//    /**
//     * Parses the next line from the file and ...
//     * @return an object encapsulating the line, or null when EOF
//     */
//    public FileLine getBigLine() throws IOException {
//        long lineLength = 0;
//        int nlPos;
//        lineHead.reset();
//        do {
//            if (currentBufferOffset >= bufferSize) {
//                buffer.clear();
//                currentBufferOffset = 0;
//                bufferSize = fileChannel.read(buffer);
//                buffer.flip();
//                if (bufferSize < 0) {
//                    break;
//                }
//            }
//            nlPos = findNewLine(buffer, currentBufferOffset, bufferSize);
//            int newChunkLen;
//            if (nlPos < 0)
//                newChunkLen = bufferSize - currentBufferOffset;
//            else
//                newChunkLen = nlPos - currentBufferOffset;
//            lineLength += newChunkLen;
//            if (lineHead.size() < Global.BUFFER_SIZE) {
//                int bytesToCopy = Math.min(Global.BUFFER_SIZE, newChunkLen);
//                bytesToCopy = Math.min(bytesToCopy, Global.BUFFER_SIZE - lineHead.size());
//                lineHead.write(buffer.array(), currentBufferOffset, bytesToCopy);
//            }
//            currentBufferOffset += newChunkLen + 1;
//        } while (nlPos < 0);
//        if (lineHead.size() == 0) {
//            return null;
//        }
//        long curStartOffset = lineStartFileOffset;
//        lineStartFileOffset += lineLength + 1;
//        if (lineLength <= Global.BUFFER_SIZE)
//            return new ShortLine(lineHead.toString());
//        else
//            return new LongLine(fileChannel, lineHead.toString(), curStartOffset, lineLength);
//    }
//
//    /**
//     * Find the first new line in a buffer
//     *
//     * @return -1 if not found
//     */
//    private static int findNewLine(ByteBuffer aBuffer, int startIndex, long bufSize) {
//        byte[] array = aBuffer.array();
//        //TODO is there an efficient char search?
//        for (int i = startIndex; i < bufSize; i++) {
//            if (arrayContains(array, i, Global.LINE_SEPARATOR_BYTES))
//                return i;
//        }
//        return -1;
//    }
//
//    private static boolean arrayContains(byte[] haystack, int offset, byte[] needle) {
//        for (int i = 0; i < needle.length; i++) {
//            if (haystack[i + offset] != needle[i])
//                return false;
//        }
//        return true;
//    }
//
//    @Override
//    public void close() throws IOException {
//        fileChannel.close();
//    }
//}
