package org.dpinol.util;

/**
 * Created by dani on 10/10/2016.
 */
@FunctionalInterface
public interface ThrowingConsumer<T> {

    void accept(T t) throws Exception;
}
