/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import java.util.ArrayList;

/**
 * A simple reusable list/array which enables adding elements and provides random access to these elements. A single instance is reusable,
 * by calling {@link #reset()}, which will not clear the underlying array, but only reset its current size.
 */
public class ReusableGrowableArray<T> {

    private final ArrayList<T> list;
    private int size = 0;

    public ReusableGrowableArray() {
        list = new ArrayList<T>();
    }

    public ReusableGrowableArray(int capacity) {
        list = new ArrayList<T>(capacity);
    }

    public void reset() {
        size = 0;
    }

    public void add(T t) {
        list.add(size++, t);
    }

    public T get(int i) {
        if (i < size) {
            return list.get(i);
        }
        throw new IndexOutOfBoundsException();
    }

    public int size() {
        return size;
    }
}
