package org.dpinol;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Merges a list of sorted files into a single one
 */
public class Merger implements AutoCloseable {
    private final List<FileLineReader> readers;
    private final BufferedWriter writer;

    /**
     * @param inputFiles should not be empty
     */
    public Merger(List<File> inputFiles, File output) throws IOException {
        readers = new ArrayList<>(inputFiles.size());
        for (File inputFile : inputFiles) {
            if (inputFile.length() > 0) {
                readers.add(new FileLineReader(inputFile));
            }
        }
        writer = new BufferedWriter(new FileWriter(output));
    }

    @Override
    public void close() throws IOException {
        for (FileLineReader reader : readers) {
            reader.close();
        }
        writer.close();
    }

    public static void mergeFiles(List<File> inputFiles, File output) throws IOException {
        try (Merger merger = new Merger(inputFiles, output)) {
            merger.merge();
        }
    }


    private static class LineWithOrigin implements Comparable<LineWithOrigin> {
        FileLine line;
        int readerIndex;

        LineWithOrigin(FileLine line, int readerIndex) {
            this.line = line;
            this.readerIndex = readerIndex;
        }

        @Override
        public int compareTo(LineWithOrigin o) {
            return line.compareTo(o.line);
        }
    }

    public void merge() throws IOException {
        int linesPushed = 0;
        Global.log("Merging " + readers.size() + " files");
        //to avoid comparing the first of each file too many times, we use a heap
        SimpleHeap<LineWithOrigin> front = new SimpleHeap<>(readers.size());
        //load heap
        int readerIndex = 0;
        for (FileLineReader reader : readers) {
            front.add(new LineWithOrigin(reader.getBigLine(), readerIndex++));
            linesPushed++;
        }

        int linesRead = 0;
        final int LOG_STEP_BYTES = 10_000_000;
        int nextLogBytes = 0;
        int bytesWritten = LOG_STEP_BYTES;
        while (!front.isEmpty()) {
            //write first line
            LineWithOrigin first = front.poll();
            bytesWritten += first.line.write(writer);
            if (bytesWritten > nextLogBytes) {
                nextLogBytes += LOG_STEP_BYTES;
            }
            writer.newLine();
            linesRead++;
            //pull next line from the used input file into the heap
            FileLineReader firstReader = readers.get(first.readerIndex);
            FileLine newLine = firstReader.getBigLine();
            if (newLine != null) {
                front.add(new LineWithOrigin(newLine, first.readerIndex));
                linesPushed++;
            }
        }
        if (linesPushed != linesRead) {
            Global.log(linesPushed + " lines pushed");
            Global.log(linesRead + " lines read");
        }
    }

}
