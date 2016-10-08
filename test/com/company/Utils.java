package com.company;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.IntStream;

import static com.company.Global.log;

/**
 * Created by dani on 29/09/16.
 */
public class Utils {
    private static final Random rnd = new Random();

    public static String createLine(int size) {
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

    static int randomSorter(Object o1, Object o2) {
        if (o1.equals(o2)) return 0;
        return (rnd.nextBoolean()) ? 1 : -1;
    }

    public static void writeRandomLines(File path, int numLines) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                Paths.get(path.getAbsolutePath())))) {
            IntStream.range(0, numLines)
                    .mapToObj(String::valueOf)
                    .sorted(Utils::randomSorter)
                    .forEach(pw::println);
        }
    }

}
