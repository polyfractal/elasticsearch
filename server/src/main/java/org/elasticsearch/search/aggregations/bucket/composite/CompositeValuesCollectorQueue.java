/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A specialized queue implementation for composite buckets
 */
final class CompositeValuesCollectorQueue implements Releasable {
    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final BigArrays bigArrays;
    private final int maxSize;
    //private final TreeMap<Integer, Integer> keys;
    private final CompositeKeyQueue queue;
    private final SingleDimensionValuesSource<?>[] arrays;
    private IntArray docCounts;
    private boolean afterKeyIsSet = false;
    private long currentHash;
    private InsertFlag lastInsertFlag = InsertFlag.NONE;
    private boolean finalized = false;
    private int finalizedSize = 0;

    private enum InsertFlag {
        NONE, UNDERFLOW, FELL_THROUGH, CONTAINS, ADDED
    }

    /**
     * Constructs a composite queue with the specified size and sources.
     *
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     * @param afterKey composite key
     */
    CompositeValuesCollectorQueue(BigArrays bigArrays, SingleDimensionValuesSource<?>[] sources, int size, CompositeKey afterKey) {
        this.bigArrays = bigArrays;
        this.maxSize = size;
        this.arrays = sources;
        //this.keys = new TreeMap<>(this::compare);
        this.queue = new CompositeKeyQueue(maxSize);
        if (afterKey != null) {
            assert afterKey.size() == sources.length;
            afterKeyIsSet = true;
            for (int i = 0; i < afterKey.size(); i++) {
                sources[i].setAfter(afterKey.get(i));
            }
        }
        this.docCounts = bigArrays.newIntArray(1, false);
    }

    /**
     * The current size of the queue.
     */
    int size() {
        return finalized ? finalizedSize : queue.size();
    }

    /**
     * Whether the queue is full or not.
     */
    boolean isFull() {
        return finalized ? finalizedSize == maxSize : queue.size() == maxSize;
    }

    /**
     * Returns a sorted {@link List} view of the slots contained in this queue.
     */
    List<Integer> getSortedSlot() {
        finalized = true;
        finalizedSize = queue.size();
        List<Integer> list = new ArrayList<>(queue.size());
        while (queue.size() > 0) {
            list.add(queue.getSlot(queue.pop()));
        }
        Collections.reverse(list);
        return list;
    }

    Integer fastCompareCurrent() {
        return queue.getSlot(currentHash);
    }


    /**
     * Returns the lowest value (exclusive) of the leading source.
     */
    Comparable<?> getLowerValueLeadSource() {
        if (finalized) {
            throw new IllegalStateException("Queue has been drained by getSortedSlot()");
        }
        return afterKeyIsSet ? arrays[0].getAfter() : null;
    }

    /**
     * Returns the upper value (inclusive) of the leading source.
     */
    Comparable<?> getUpperValueLeadSource() throws IOException {
        if (finalized) {
            throw new IllegalStateException("Queue has been drained by getSortedSlot()");
        }
        return size() >= maxSize ? arrays[0].toComparable(queue.getSlot(queue.top())) : null;
    }
    /**
     * Returns the document count in <code>slot</code>.
     */
    int getDocCount(int slot) {
        return docCounts.get(slot);
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts = bigArrays.grow(docCounts, slot+1);
        docCounts.set(slot, 1);
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareCurrentWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    private void calculateCurrentHash() {
        long hash = 0;
        for (SingleDimensionValuesSource<?> array : arrays) {
            hash = 31 * hash + array.currentHash();
            //hash = array.currentHash();
        }
        currentHash = hash;
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector in) throws IOException {
        return getLeafCollector(null, context, in);
    }
    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * If <code>forceLeadSourceValue</code> is not null, the leading source will use this value
     * for each document.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(Comparable<?> forceLeadSourceValue,
                                         LeafReaderContext context, LeafBucketCollector in) throws IOException {
        int last = arrays.length - 1;
        LeafBucketCollector collector = in;
        while (last > 0) {
            collector = arrays[last--].getLeafCollector(context, collector);
        }
        if (forceLeadSourceValue != null) {
            collector = arrays[last].getLeafCollector(forceLeadSourceValue, context, collector);
        } else {
            collector = arrays[last].getLeafCollector(context, collector);
        }
        return collector;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return The target slot of the candidate or -1 is the candidate is not competitive.
     */
    int addIfCompetitive() {
        // Do we already have this key?
        calculateCurrentHash();
        Integer topSlot = queue.getSlot(currentHash);
        if (topSlot != null) {
            docCounts.increment(topSlot, 1);
            return topSlot;
        }

        // checks if the candidate key is competitive
        if (queue.compareTop() == false) {
            return -1;
        }

        if (afterKeyIsSet && compareCurrentWithAfter() <= 0) {
            // this key is greater than the top value collected in the previous round, skip it
            return -1;
        }

        // the candidate key is competitive
        final int newSlot = queue.insertWithOverflowFlagged(currentHash);
        switch (lastInsertFlag) {
            case CONTAINS:
                // This should be taken care of before inserting, but just in case
                docCounts.increment(newSlot, 1);
                return newSlot;
            case FELL_THROUGH:
                return -1;
            case ADDED:
            case UNDERFLOW:
                assert newSlot < maxSize;
                // move the candidate key to its new slot
                copyCurrent(newSlot);
                return newSlot;
            case NONE:
            default:
                return -1;
        }
    }

    @Override
    public void close() {
        Releasables.close(docCounts);
    }

    private class CompositeKeyQueue extends PriorityQueue<Long> {

        private HashMap<Long, Integer> hashToSlot;
        public CompositeKeyQueue(int maxSize) {
            super(maxSize);
            hashToSlot = new HashMap<>(maxSize);
        }

        public Integer getSlot(long hash) {
            return hashToSlot.get(hash);
        }

        public boolean compareTop() {
            if (size() < maxSize) {
                return true;
            }

            int slot = hashToSlot.get(top());
            for (SingleDimensionValuesSource<?> array : arrays) {
                int cmp = array.compareCurrent(slot);
                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            // all equal means it's the same key
            return true;
        }

        @Override
        protected boolean lessThan(Long hash1, Long hash2) {
            for (SingleDimensionValuesSource<?> array : arrays) {
                int cmp;
                if (hash1 == currentHash) {
                    cmp = array.compareCurrent(hashToSlot.get(hash2));
                } else if (hash2 == currentHash) {
                    cmp = -array.compareCurrent(hashToSlot.get(hash1));
                } else {
                    cmp = array.compare(hashToSlot.get(hash1), hashToSlot.get(hash2));
                }

                if (cmp != 0) {
                    return cmp > 0;
                }
            }
            return true;
        }


        public int insertWithOverflowFlagged(Long element) {
            if (size() < maxSize) {
                super.add(currentHash);
                hashToSlot.put(currentHash, size() - 1);
                lastInsertFlag = InsertFlag.UNDERFLOW;
                return size() - 1;
            }

            Integer slot = hashToSlot.get(currentHash);
            if (slot != null) {
                lastInsertFlag = InsertFlag.CONTAINS;
                return slot;
            }

            Long e = super.insertWithOverflow(currentHash);
            if (e.equals(element)) {
                lastInsertFlag = InsertFlag.FELL_THROUGH;
                return -1;
            }

            slot = hashToSlot.remove(e);
            hashToSlot.put(currentHash, slot);
            lastInsertFlag = InsertFlag.ADDED;
            return slot;

        }
    }
}
