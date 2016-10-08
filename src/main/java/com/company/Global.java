package com.company;

import java.util.Date;

/**
 * Created by dani on 23/09/16.
 */
public class Global {
    /**
     * With 256, it takes 19.5s
     * With 1k-14k, it takes 14.1-15.5s to sort file 800M with only short lines
     * With 8k, 14.xs
     * With 100k, 13.5
     * With 500k, 14,8
     */
    public static final int BUFFER_SIZE = 1 * 1024;
    public static final String ENCODING = "UTF-8";
    public static final String LINE_SEPARATOR = java.security.AccessController.doPrivileged(
            new sun.security.action.GetPropertyAction("line.separator"));
    public static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes();

    public static void log(String str) {
        System.out.println(new Date() +  ": " + str);
    }
}
