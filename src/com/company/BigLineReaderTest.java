package com.company;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import static com.company.global.BUFFER_SIZE;
import static org.junit.Assert.*;

/**
 * Created by dani on 26/09/16.
 */
public class BigLineReaderTest {
    File tempFile;


    @org.junit.Before
    public void setUp() throws Exception {
        tempFile = File.createTempFile("BigLineReaderTest", "test");
        tempFile.deleteOnExit();
    }

    String createLine(int size) {
        StringBuilder sb = new StringBuilder(size);
        char c = 'a';
        for (int i = 0; i < size; i++) {
            sb.append(c);
            c++;
            if (c > 'z')
                c = 'a';
        }
        return sb.toString();
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