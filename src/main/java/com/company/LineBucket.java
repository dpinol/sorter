package com.company;

import java.util.ArrayList;

/**
 * Created by dani on 06/10/2016.
 */
class LineBucket extends ArrayList<BigLine> {
    LineBucket() {
        super(BigFileSorter.QUEUE_BUCKET_SIZE);
    }

    boolean isFull() {
        return size() == BigFileSorter.QUEUE_BUCKET_SIZE;
    }
}
