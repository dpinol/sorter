package org.dpinol.function;

/**
 * Same as {@link java.util.function.Consumer}, but allowing exceptions
 */

@FunctionalInterface
public interface ThrowingConsumer<T> {

    void accept(T t) throws Exception;
}
