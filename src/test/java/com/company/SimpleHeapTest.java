package com.company;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by dani on 04/10/2016.
 */
public class SimpleHeapTest {

    @Test
    public void increasing() {
        int SIZE = 10;
        SimpleHeap<Integer> sut = new SimpleHeap<>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            sut.add(i);
        }
        assertInOrder(sut, SIZE);
    }

    private void assertInOrder(SimpleHeap<Integer> sut, int size) {
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(i, sut.poll().intValue());
        }

    }

    @Test
    public void decreasing() {
        int SIZE = 10;
        SimpleHeap<Integer> sut = new SimpleHeap<>(SIZE);
        for (int i = SIZE - 1; i >= 0; i--) {
            sut.add(i);
        }

        assertInOrder(sut, SIZE);
    }

    @Test
    public void shuffled() {
        //GIVEN
        int SIZE = 10;
        IntStream shuffledInts = Utils.shuffledIntRange(SIZE);
        SimpleHeap<Integer> sut = new SimpleHeap<>(SIZE);
        //WHEN
        shuffledInts.forEach(sut::add);
        //THEN
        assertInOrder(sut, SIZE);
    }

}