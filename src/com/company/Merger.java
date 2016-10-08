package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by dani on 20/09/16.
 */
public class Merger implements AutoCloseable {
    private final List<File> inputFiles;
    private final File output;
    private final List<BigLineReader> readers;
    private final BufferedWriter writer;

    /**
     * @param inputFiles should not be empty
     */
    public Merger(List<File> inputFiles, File output) throws IOException {
        this.inputFiles = inputFiles;
        this.output = output;
        readers = new ArrayList<>(inputFiles.size());
        for (File inputFile : inputFiles) {
            if (inputFile.length() > 0) {
                readers.add(new BigLineReader(inputFile));
            }
        }
        writer = new BufferedWriter(new FileWriter(output));
    }

    @Override
    public void close() throws Exception {
        for (BigLineReader reader : readers) {
            reader.close();
        }
        writer.close();
    }


    private static class LineWithOrigin implements Comparable<LineWithOrigin> {
        BigLine line;
        int readerIndex;

        LineWithOrigin(BigLine line, int readerIndex) {
            this.line = line;
            this.readerIndex = readerIndex;
        }

        @Override
        public int compareTo(LineWithOrigin o) {
            return line.compareTo(o.line);
        }
    }

    void merge() throws IOException {
        int linesPushed = 0;
        Global.log("Merging " + readers.size() + " files");
        //to avoid comparing the first of each file too many times, we use a heap
        PriorityQueue<LineWithOrigin> front = new PriorityQueue<>();
        //load heap
        int readerIndex = 0;
        for (BigLineReader reader : readers) {
            front.add(new LineWithOrigin(reader.getBigLine(), readerIndex++));
            linesPushed++;
        }

        int numDrainedFiles = 0;
        int linesRead = 0;
        int logStep = readers.size() / 10;
        while (!front.isEmpty()) {
            LineWithOrigin first = front.poll();
            first.line.write(writer);
            writer.newLine();
            linesRead++;
            BigLineReader firstReader = readers.get(first.readerIndex);
            BigLine newLine = firstReader.getBigLine();
            if (newLine != null) {
                front.add(new LineWithOrigin(newLine, first.readerIndex));
                linesPushed++;
            } else {
                numDrainedFiles++;
                if (numDrainedFiles % logStep == 0) {
                    Global.log("Completed " + numDrainedFiles + "/" + readers.size());
                }
            }
        }
        Global.log(linesPushed + " lines pushed");
        Global.log(linesRead + " lines read");

    }

}
