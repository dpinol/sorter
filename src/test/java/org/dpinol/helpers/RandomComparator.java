package org.dpinol.helpers;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by dani on 09/10/2016.
 */
public final class RandomComparator<T> implements Comparator<T> {

    private final Map<T, Integer> map = new IdentityHashMap<>();
    private final Random random;

    public RandomComparator() {
        this(new Random());
    }

    public RandomComparator(Random random) {
        this.random = random;
    }

    @Override
    public int compare(T t1, T t2) {
        return Integer.compare(valueFor(t1), valueFor(t2));
    }

    private int valueFor(T t) {
        synchronized (map) {
            return map.computeIfAbsent(t, ignore -> random.nextInt());
        }
    }

}