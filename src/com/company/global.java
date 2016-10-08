package com.company;

import java.util.Date;

/**
 * Created by dani on 23/09/16.
 */
public class global {
    /**
     * With 1k, it takes 1min53s to sort file 800M with only short lines
     */
    public static final int BUFFER_SIZE = 10 * 1024;
    public static final String ENCODING = "UTF-8";
    public static final String LINE_SEPARATOR = java.security.AccessController.doPrivileged(
            new sun.security.action.GetPropertyAction("line.separator"));

    public static void log(String str) {
        System.out.println(new Date() +  ": " + str);
    }
}
