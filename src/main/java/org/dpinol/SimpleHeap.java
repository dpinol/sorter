package org.dpinol;

/**
 * Heap to sort a collection of objects
 */
public class SimpleHeap<E extends Comparable<E>> {
    private int size = 0;
    private Object[] data;
    private final int capacity;

    /**
     *
     * @param capacity maximum number of objects to sort
     */
    public SimpleHeap(int capacity) {
        data = new Object[capacity];
        this.capacity = capacity;
    }

    @SuppressWarnings("unchecked")
    private E get(int pos) {
        return (E) data[pos];
    }

    public void add(E item) {
        if (size >= capacity) {
            throw new IllegalStateException("Head is full");
        }
        int k = size;
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            E e = get(parent);
            if (item.compareTo((E) e) >= 0)
                break;
            data[k] = e;
            k = parent;
        }
        data[k] = item;
        size++;
    }


    /**
     * @return null when empty
     */
    public E poll() {
        if (size == 0) {
            return null;
        }
        int s = --size;
        E result = get(0);
        E x = get(s);
        data[s] = null;
        if (s != 0) {
            moveDown(x);
        }
        return result;
    }

    private void moveDown(E x) {
        int k = 0;
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            E c = get(child);
            int right = child + 1;
            if (right < size && c.compareTo(get(right)) > 0) {
                c = get(child = right);
            }
            if (x.compareTo(c) <= 0)
                break;
            data[k] = c;
            k = child;
        }
        data[k] = x;
    }


    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isFull() {
        return capacity == size;
    }
}
