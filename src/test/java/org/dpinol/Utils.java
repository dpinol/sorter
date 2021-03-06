package org.dpinol;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.IntStream;

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

    /**
     * Useful to shuffle a collection
     */
    static int randomSorter(Object o1, Object o2) {
        if (o1.equals(o2)) return 0;
        return (rnd.nextBoolean()) ? 1 : -1;
    }

    public static void writeRandomLines(File path, int numLines, int minLineLen) throws IOException {
        char sufChars[] = new char[minLineLen];
        for (int i = 0; i < minLineLen; i++) {
            sufChars[i] = 'p';
        }
        String suf = new String(sufChars);
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                Paths.get(path.getAbsolutePath())))) {
            IntStream.range(0, numLines)
                    .mapToObj(String::valueOf)
                    .map(num -> num + suf)
                    .sorted(Utils::randomSorter)
                    .forEach(pw::println);
        }
    }

    /**
     * creates stream from 0 to n-1, and returns it shuffled
     * @param n
     * @return
     */
    public static IntStream shuffledIntRange(int n) {
        return IntStream.range(0, n)
                .mapToObj(Integer::valueOf)
                .sorted(Utils::randomSorter)
                .mapToInt(x -> x);
    }

}
