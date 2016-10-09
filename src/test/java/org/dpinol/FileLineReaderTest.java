package org.dpinol;

import org.dpinol.helpers.Timer;
import org.dpinol.util.Log;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Random;

import static org.dpinol.Global.BUFFER_SIZE;
import static org.dpinol.Global.LINE_SEPARATOR_BYTES;
import static org.dpinol.helpers.Utils.createLine;
import static org.junit.Assert.*;

/**
 * Created by dani on 26/09/16.
 */
public class FileLineReaderTest {
    private File tempFile;
    private final Random rnd = new Random();

    @Before
    public void setUp() throws Exception {
        tempFile = File.createTempFile("FileLineReaderTest", "test");
        tempFile.deleteOnExit();
    }


    @Test
    public void shortLines() throws Exception {
        writeAndRead("line1", "line2", "line3");
    }

    @Test
    public void longLines() throws Exception {
        String lines[] = {createLine(BUFFER_SIZE),
                createLine(BUFFER_SIZE + 1),
                createLine(BUFFER_SIZE * 2),
                createLine(BUFFER_SIZE * 2 + 1)};
        writeAndRead(lines);
    }


    @Test
    public void longRndLines() throws Exception {
        String lines[] = new String[50];
        for (int i = 0; i < lines.length; i++) {
            lines[i] = createLine(rnd.nextInt(BUFFER_SIZE * 3));
        }
        writeAndRead(lines);
    }


    private int findNewLine(ByteBuffer aBuffer, int startIndex, long bufSize) {
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


    @Ignore
    @Test
    public void performanceFC() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        try (FileChannel fileChannel = FileChannel.open(Paths.get("/Users/dani/appDev/shibs/input.txt"));
             Timer timer = new Timer()) {
            int read = 0;
            int total = 0;
            int numLines = 0;
            while ((read = fileChannel.read(buffer)) >= 0) {
                total += read;
                buffer.flip();
                int nl = -1;
                while ((nl = findNewLine(buffer, nl + 1, buffer.limit())) >= 0)
                    numLines++;
                if (total % 100_000 == 0)
                    Log.info("buffers read: " + total + ", lines:" + numLines);
                buffer.clear();
            }
            Log.info("Num lines:" + numLines);
        }
    }


    @Ignore
    @Test
    public void performanceBL() throws Exception {
        try (FileLineReader reader = new FileLineReader(new File("/Users/dani/appDev/shibs/input.txt"));
             Timer timer = new Timer()) {
            int i = 0;
            FileLine bl;
            while ((bl = reader.readFileLine()) != null) {
                Iterator<String> it = bl.getIterator();
                while (it.hasNext()) {
                    it.next();
                }
                if (++i % 100_000 == 0)
                    Log.info("lines read: " + i);
            }
        }
    }

    @Test
    public void longLines10BL() throws Exception {
        try (FileLineReader reader = new FileLineReader(new File("/Users/dani/appDev/shibs/10_long_lines.txt"));
             Timer timer = new Timer()) {
            int i = 0;
            FileLine bl;
            while ((bl = reader.readFileLine()) != null) {
                Iterator<String> iterator = bl.getIterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
//                    Log.info(next);
                }
                Log.info("lines read: " + (i++));
            }
        }

    }

    @Ignore
    @Test
    public void performanceStream() throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader("/Users/dani/appDev/shibs/input.txt"));
             Timer timer = new Timer()) {
            int i = 0;
            while (reader.readLine() != null) {
                if (++i % 100_000 == 0)
                    Log.info("lines read: " + i);
            }
        }
    }


    /**
     * Writes the lines to a single file
     * reads file with FileLineReader, verifying it reads the written lines
     */
    private void writeAndRead(String... lines2write) throws Exception {
        writeLines(lines2write);
        try (FileLineReader reader = new FileLineReader(tempFile)) {
            for (String line : lines2write) {
                Log.info("testing line of length " + line.length());
                String readLine = readLine(reader);
                assertFalse(readLine.contains(Global.LINE_SEPARATOR));
                assertEquals("expected length", line.length(), readLine.length());
                assertEquals(line, readLine);
            }
            assertNull(reader.readFileLine());
        }
    }

    private String readLine(FileLineReader reader) throws Exception {
        FileLine fileLine = reader.readFileLine();
        Iterator<String> iterator = fileLine.getIterator();
        StringBuilder builder = new StringBuilder();
        while (iterator.hasNext()) {
            builder.append(iterator.next());
        }
        return builder.toString();
    }

    private void writeLines(String... lines) throws IOException {
        try {
            FileWriter writer = new FileWriter(tempFile);
            for (String line : lines) {
                writer.write(line + "\n");
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}