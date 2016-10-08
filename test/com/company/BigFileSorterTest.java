package com.company;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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
        BigFileSorter bigFileSorter = new BigFileSorter(new File("/Users/dani/appDev/shibs/2_long_lines.txt"),
                new File("/Users/dani/appDev/shibs/2_long_lines.out"), null);
        bigFileSorter.sort();

    }


    @Test
    public void shortShuffle() throws Exception {
        Utils.writeRandomLines(inputFile, 5);
        BigFileSorter bigFileSorter = new BigFileSorter(inputFile, outputFile, null);
        bigFileSorter.sort();

    }


}