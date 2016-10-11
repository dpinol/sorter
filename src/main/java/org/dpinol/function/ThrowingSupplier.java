package org.dpinol.function;

/**
 * Same as {@link java.util.function.Supplier}, but allowing exceptions
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}
