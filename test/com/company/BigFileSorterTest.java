package com.company;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import java.io.*;

/**
 * Created by dani on 28/09/16.
 */
public class BigFileSorterTest {

    private File inputFile, outputFile;

    @Before
    public void setup() throws IOException {
        inputFile = File.createTempFile("BigFileSorterTest_input", null);
        inputFile.deleteOnExit();
        outputFile = File.createTempFile("BigFileSorterTest_output", null);
        outputFile.deleteOnExit();

    }

    @Ignore
    @Test
    public void test() throws Exception {
        BigFileSorter bigFileSorter = new BigFileSorter(new File("/Users/dani/appDev/shibs/10_long_lines.txt"),
                new File("/Users/dani/appDev/shibs/10_long_lines.out"), null);
        bigFileSorter.sort();
    }


    @Test
    public void shortShuffle() throws Exception {
        Utils.writeRandomLines(inputFile, 5, 3);
        BigFileSorter bigFileSorter = new BigFileSorter(inputFile, outputFile, null);
        bigFileSorter.sort();
    }

    @Test
    public void longShuffle() throws Exception {
        int NUM_LINES = 5;
        int MIN_LEN = Global.BUFFER_SIZE;
        Utils.writeRandomLines(inputFile, NUM_LINES, MIN_LEN);
        BigFileSorter bigFileSorter = new BigFileSorter(inputFile, outputFile, null);
        bigFileSorter.sort();
        checkLines(outputFile, NUM_LINES, MIN_LEN);
    }

    void checkLines(File outputFile, int numLines, int minLen) throws IOException {
        int readLines = 0;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                readLines++;
                Assert.assertThat(line, line.length(), Matchers.greaterThan(minLen));
            }
        }
        Assert.assertEquals(numLines, readLines);
    }

}