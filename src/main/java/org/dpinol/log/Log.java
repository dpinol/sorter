package org.dpinol.log;

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

    static final Level level = Level.INFO;
    private final String name;

    public Log(String name) {
        this.name = name;
    }

    public Log(Class clazz) {
        this.name = clazz.getSimpleName();
    }


    public void error(String str, Object... args) {
        log(Level.ERROR, str, args);
    }

    public void info(String str, Object... args) {
        log(Level.INFO, str, args);
    }

    public void debug(String str, Object... args) {
        log(Level.DEBUG, str, args);
    }

    public void log(Level level, String str, Object... args) {
        if (level.ordinal() >= Log.level.ordinal()) {
            System.out.println(new Date() + ": " + name + ": " + String.format(str, args));
        }
    }


}
