//package org.dpinol;
//
//import org.dpinol.util.Log;
//
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.AsynchronousFileChannel;
//import java.nio.channels.CompletionHandler;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//import java.nio.file.attribute.FileAttribute;
//import java.util.Collections;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicLong;
//
//import static org.dpinol.Global.BUFFER_SIZE;
//
///**
// * Created by dani on 08/10/2016.
// */
//public class AsyncFileLineReader implements AutoCloseable {
//    //Use java.util.concurrent.Executor to run the tasks submitted
//    //creating an an asynchronous channel group with a fixed thread pool.
//    private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(6);
//    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
//    private final AsynchronousFileChannel afc;
//    private final Path input;
//    private long fileOffset = 0;
//    private int currentBufferOffset = 0;
//    private int bufferSize = 0;
//    private final ByteArrayOutputStream lineHead = new ByteArrayOutputStream(Global.BUFFER_SIZE);
//    private CompletableFuture<Integer> readBufferCf;
//    /**
//     * does not include \n
//     */
//    private final AtomicLong lineLength = new AtomicLong(0);
//
//
//    public AsyncFileLineReader(File input) throws IOException {
//        this(Paths.get(input.toString()));
//    }
//
//    public AsyncFileLineReader(Path input) throws IOException {
//        afc = AsynchronousFileChannel.open(input, Collections.singleton(StandardOpenOption.READ),
//                executor, new FileAttribute[0]);
//        this.input = input;
//        readBufferAsync();
//    }
//
//
//    public void readBufferAsync() {
//        readBufferCf = new CompletableFuture<>();
//        buffer.clear();
//        currentBufferOffset = 0;
//        Log.debug("Reading at %d", fileOffset);
//        afc.read(buffer, fileOffset, readBufferCf, new CompletionHandler<Integer, CompletableFuture<Integer>>() {
//            @Override
//            public void completed(Integer bytesRead, CompletableFuture<Integer> cf) {
//                fileOffset += bytesRead;
//                cf.complete(bytesRead);
//            }
//
//            @Override
//            public void failed(Throwable exc, CompletableFuture<Integer> cf) {
//                cf.completeExceptionally(exc);
//            }
//        });
//    }
//
//    public CompletableFuture<FileLine> read() {
//        CompletableFuture<FileLine> lineCf = new CompletableFuture<>();
//        CompletableFuture.runAsync(() -> {
//            try {
//                FileLine fileLine = readCore();
//                if (fileLine != null) {
//                    Log.debug("Returning line of " + fileLine.getNumBytes() + " bytes");
//                } else {
//                    Log.debug("Returning null line");
//                }
//                lineCf.complete(fileLine);
//            } catch (Exception e) {
//                lineCf.completeExceptionally(e);
//            }
//        }, executor);
//        return lineCf;
//    }
//
//    private FileLine readCore() throws Exception {
//        lineHead.reset();
//        lineLength.set(0);
////        if (bufferSize < 0) {
////            Log.debug("bufferSize < 0");
////            return null;
////        }
//        int nlPos;
//        bufferSize = readBufferCf.get();
//        long curStartOffset = fileOffset - bufferSize + currentBufferOffset;
//        do {
//            bufferSize = readBufferCf.get();
//            if (bufferSize <= 0) {
//                Log.debug("bufferSize <=0 {}", bufferSize);
//                break;
//            }
//            nlPos = parseLine(lineLength);
//        } while (nlPos < 0);
//        if (lineHead.size() == 0) {
//            Log.debug("empty linehead");
//            return null;
//        }
//        if (lineLength.get() <= Global.BUFFER_SIZE)
//            return new ShortLine(lineHead.toString());
//        else
//            return new LongLine(afc, lineHead.toString(), curStartOffset, lineLength.get());
//    }
//
//    int parseLine(AtomicLong lineLength) {
//        int nlPos = findNewLine(buffer, currentBufferOffset, bufferSize);
//        int newChunkLen;
//        if (nlPos < 0)
//            newChunkLen = bufferSize - currentBufferOffset;
//        else
//            newChunkLen = nlPos - currentBufferOffset;
//        lineLength.addAndGet(newChunkLen);
//        if (lineHead.size() < Global.BUFFER_SIZE) {
//            int bytesToCopy = Math.min(Global.BUFFER_SIZE, newChunkLen);
//            bytesToCopy = Math.min(bytesToCopy, Global.BUFFER_SIZE - lineHead.size());
////            Log.info(bufferSize +" "+ currentBufferOffset + " " + lineHead.size());
//            lineHead.write(buffer.array(), currentBufferOffset, bytesToCopy);
//        }
//        if (nlPos < 0 && bufferSize > 0) {
//            readBufferAsync();
//        } else {
//            currentBufferOffset += newChunkLen + 1;
//        }
//        return nlPos;
//    }
//
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
//        afc.close();
//    }
//}
