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

import java.util.*;

/**
 *
 */
public final class Casts {

    private Casts() {}
    
    public static <T, S extends T> List<T> castImmutable(List<S> supers, Class<T> type) {
        return new ImmutableCastingList<T, S>(type, supers);
    }

    public static <T, S extends T> Iterator<T> castImmutable(Iterator<S> supers, Class<T> type) {
        return new ImmutableIterator<T, S>(supers);
    }

    public static <K, SK extends K, V, SV extends V> Map<K, V> castImmutable(Map<SK, SV> supers, Class<K> keyType, Class<V> valueType) {
        return new ImmutableCastingMap<K, SK, V, SV>(keyType, valueType, supers);
    }


    static class ImmutableCastingCollection<T, S extends T> implements Collection<T> {

        private final Collection<S> col;

        ImmutableCastingCollection(Collection<S> col) {
            this.col = col;
        }

        @Override
        public int size() {
            return col.size();
        }

        @Override
        public boolean isEmpty() {
            return col.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return col.contains(o);
        }

        @Override
        public Iterator<T> iterator() {
            return new ImmutableIterator<T, S>(col.iterator());
        }

        @Override
        public Object[] toArray() {
            return col.toArray();
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            return col.toArray(a);
        }

        @Override
        public boolean add(T t) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return col.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("This list is immutable");
        }

    }

    static class ImmutableCastingList<T, S extends T> implements List<T> {

        private final Class<T> type;
        private final List<S> list;

        ImmutableCastingList(Class<T> type, List<S> list) {
            this.type = type;
            this.list = list;
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public boolean isEmpty() {
            return list.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return list.contains(o);
        }

        @Override
        public Iterator<T> iterator() {
            return new ImmutableIterator<T, S>(list.iterator());
        }

        @Override
        public Object[] toArray() {
            return list.toArray(); 
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            return list.toArray(a);
        }

        @Override
        public boolean add(T t) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return list.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean addAll(int index, Collection<? extends T> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public T get(int index) {
            return list.get(index);
        }

        @Override
        public T set(int index, T element) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public void add(int index, T element) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public T remove(int index) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public int indexOf(Object o) {
            return list.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return list.lastIndexOf(o);
        }

        @Override
        public ListIterator<T> listIterator() {
            return new ImmutableListIterator<T, S>(list.listIterator());
        }

        @Override
        public ListIterator<T> listIterator(int index) {
            return new ImmutableListIterator<T, S>(list.listIterator(index));
        }

        @Override
        public List<T> subList(int fromIndex, int toIndex) {
            return new ImmutableCastingList<T, S>(type, list.subList(fromIndex, toIndex));
        }
    }

    static class ImmutableIterator<T, S extends T> implements Iterator<T> {

        protected final Iterator<S> iterator;

        ImmutableIterator(Iterator<S> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator is immutable");
        }
    }

    static class ImmutableListIterator<T, S extends T> extends ImmutableIterator<T, S> implements ListIterator<T> {

        ImmutableListIterator(ListIterator<S> iterator) {
            super(iterator);
        }

        @Override
        public boolean hasPrevious() {
            return ((ListIterator) iterator).hasPrevious();
        }

        @Override
        public T previous() {
            return (T) ((ListIterator) iterator).previous();
        }

        @Override
        public int nextIndex() {
            return ((ListIterator) iterator).nextIndex();
        }

        @Override
        public int previousIndex() {
            return ((ListIterator) iterator).previousIndex();
        }

        @Override
        public void set(T t) {
            throw new UnsupportedOperationException("This iterator is immutable");
        }

        @Override
        public void add(T t) {
            throw new UnsupportedOperationException("This iterator is immutable");
        }
    }

    static class ImmutableCastingSet<T, S extends T> implements Set<T> {

        private final Set<S> set;

        ImmutableCastingSet(Set<S> set) {
            this.set = set;
        }

        @Override
        public int size() {
            return set.size();
        }

        @Override
        public boolean isEmpty() {
            return set.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return set.contains(o);
        }

        @Override
        public Iterator<T> iterator() {
            return new ImmutableIterator<T, S>(set.iterator());
        }

        @Override
        public Object[] toArray() {
            return set.toArray();
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            return set.toArray(a);
        }

        @Override
        public boolean add(T t) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return set.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("This list is immutable");
        }
    }
    
    static class ImmutableCastingMap<K, SK extends K, V, SV extends V> implements Map<K, V> {

        private final Class<K> keyType;
        private final Class<V> valType;
        private final Map<SK, SV> map;

        ImmutableCastingMap(Class<K> keyType, Class<V> valType, Map<SK, SV> map) {
            this.keyType = keyType;
            this.valType = valType;
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return map.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return map.containsValue(value);
        }

        @Override
        public V get(Object key) {
            return map.get(key);
        }

        @Override
        public V put(K key, V value) {
            throw new UnsupportedOperationException("This msp is immutable");
        }

        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException("This msp is immutable");
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException("This msp is immutable");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("This msp is immutable");
        }

        @Override
        public Set<K> keySet() {
            return new ImmutableCastingSet<K, SK>(map.keySet());
        }

        @Override
        public Collection<V> values() {
            return new ImmutableCastingCollection<V, SV>(map.values());
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return new ImmutableCastingEntrySet<K, SK, V, SV>(map.entrySet());
        }

        static class ImmutableCastingEntry<K, SK extends K, V, SV extends V> implements Entry<K, V> {

            private final Entry<SK, SV> entry;

            ImmutableCastingEntry(Entry<SK, SV> entry) {
                this.entry = entry;
            }

            @Override
            public K getKey() {
                return entry.getKey();
            }

            @Override
            public V getValue() {
                return entry.getValue();
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException("This map entry is immutable");
            }
        }

        static class ImmutableCastingEntrySet<K, SK extends K, V, SV extends V> implements Set<Map.Entry<K, V>> {

            private final Set<Map.Entry<SK, SV>> set;

            ImmutableCastingEntrySet(Set<Entry<SK, SV>> set) {
                this.set = set;
            }

            @Override
            public int size() {
                return set.size();
            }

            @Override
            public boolean isEmpty() {
                return set.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return set.contains(o);
            }

            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                return new ImmutableCastingEntryIterator<K, SK, V, SV>(set.iterator());
            }

            @Override
            public Object[] toArray() {
                return set.toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return set.toArray(a);
            }

            @Override
            public boolean add(Entry<K, V> kvEntry) {
                throw new UnsupportedOperationException("This entry set is immutable");
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException("This entry set is immutable");
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                return set.contains(c);
            }

            @Override
            public boolean addAll(Collection<? extends Entry<K, V>> c) {
                throw new UnsupportedOperationException("This entry set is immutable");
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException("This entry set is immutable");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException("This entry set is immutable");
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("This entry set is immutable");
            }
        }
    }

    static class ImmutableCastingEntryIterator<K, SK extends K, V, SV extends V> implements Iterator<Map.Entry<K, V>> {

        private final Iterator<Map.Entry<SK, SV>> iterator;

        ImmutableCastingEntryIterator(Iterator<Map.Entry<SK, SV>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Map.Entry<K, V> next() {
            return new ImmutableCastingMap.ImmutableCastingEntry<K, SK, V, SV>(iterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator is immutable");
        }
    }

}
