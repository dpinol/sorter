package org.dpinol.util;

/**
 * Created by dani on 10/10/2016.
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}
