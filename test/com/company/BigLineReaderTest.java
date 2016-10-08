package com.company;

import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

import static com.company.Global.*;
import static com.company.Utils.createLine;
import static org.junit.Assert.*;

/**
 * Created by dani on 26/09/16.
 */
public class BigLineReaderTest {
    private File tempFile;


    @org.junit.Before
    public void setUp() throws Exception {
        tempFile = File.createTempFile("BigLineReaderTest", "test");
        tempFile.deleteOnExit();
    }


    @Test
    public void shortLines() throws Exception {
        writeAndRead("line1", "line2", "line3");
    }

    @Test
    public void longLines() throws Exception {
        String line1 = createLine(BUFFER_SIZE - 1);
        String line2 = createLine(BUFFER_SIZE);
        String line3 = createLine(BUFFER_SIZE + 1);
        writeAndRead(line1, line2, line3);
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
                    Global.log("buffers read: " + total + ", lines:" + numLines);
                buffer.clear();
            }
            Global.log("Num lines:" + numLines);
        }
    }


    @Ignore
    @Test
    public void performanceBL() throws Exception {
        try (BigLineReader reader = new BigLineReader(new File("/Users/dani/appDev/shibs/input.txt"));
             Timer timer = new Timer()) {
            int i = 0;
            BigLine bl;
            while ((bl = reader.getBigLine()) != null) {
                Iterator<String> it = bl.getIterator();
                while (it.hasNext()) {
                    it.next();
                }
                if (++i % 100_000 == 0)
                    Global.log("lines read: " + i);
            }
        }
    }

    @Test
    public void longLines10BL() throws Exception {
        try (BigLineReader reader = new BigLineReader(new File("/Users/dani/appDev/shibs/10_long_lines.txt"));
             Timer timer = new Timer()) {
            int i = 0;
            BigLine bl;
            while ((bl = reader.getBigLine()) != null) {
                Iterator<String> iterator = bl.getIterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
//                    Global.log(next);
                }
                Global.log("lines read: " + (i++));
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
                    Global.log("lines read: " + i);
            }
        }
    }


    private void writeAndRead(String... lines2write) throws IOException {
        writeLines(lines2write);
        try (BigLineReader reader = new BigLineReader(tempFile)) {
            for (String line : lines2write) {
                String readLine = readLine(reader);
                assertEquals(line, readLine);
            }
            assertNull(reader.getBigLine());
        }
    }

    private String readLine(BigLineReader reader) throws IOException {
        BigLine bigLine = reader.getBigLine();
        Iterator<String> iterator = bigLine.getIterator();
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