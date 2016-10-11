package org.dpinol.data;

/**
 * Heap to sort a collection of {@link Comparable} objects ascendingly
 * See algorithm explanation at https://www.tutorialspoint.com/data_structures_algorithms/heap_data_structure.htm
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

    /**
     * Pushes an item to a suitable position
     */
    public void add(E item) {
        if (size >= capacity) {
            throw new IllegalStateException("Head is full");
        }
        int index = size;
        while (index > 0) {
            int parent = (index - 1) >>> 1;
            E e = get(parent);
            if (item.compareTo(e) >= 0)
                break;
            data[index] = e;
            index = parent;
        }
        data[index] = item;
        size++;
    }


    /**
     * Remove the first (smallest)
     * @return null when empty
     */
    public E poll() {
        if (size == 0) {
            return null;
        }
        final int newSize = --size;
        final E topElement = get(0);
        final E lastElement = get(newSize);
        data[newSize] = null;
        if (newSize != 0) {
            pushDown(lastElement);
        }
        return topElement;
    }

    /**
     * Pushes down the specified element from first position until one where it's not larger than parent
     */
    private void pushDown(E x) {
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
