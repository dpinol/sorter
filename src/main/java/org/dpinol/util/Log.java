package org.dpinol.util;

import java.util.Date;

/**
 * Created by dani on 09/10/2016.
 */
public class Log {
    enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    static Level level = Level.INFO;

    public static void info(String str, Object... args) {
        log(Level.INFO, str, args);
    }

    public static void debug(String str, Object... args) {
        log(Level.DEBUG, str, args);
    }

    public static void log(Level level, String str, Object... args) {
        if (level.ordinal() >= Log.level.ordinal())
            System.out.println(new Date() + ": " + String.format(str, args));
    }


}
