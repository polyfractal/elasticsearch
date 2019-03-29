package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SetBackedScalingCuckooFilter implements Writeable {

    private static final int FILTER_CAPACITY = 1000000;
    private static final double FPP = 0.01;

    // Package-private for testing
    Set<MurmurHash3.Hash128> hashes;
    List<CuckooFilter> filters;

    private final int threshold;
    private final Random rng;
    private final int capacity;
    private final double fpp;
    private Consumer<Long> breaker = aLong -> {
        //noop
    };
    private boolean isSetMode = true;

    public SetBackedScalingCuckooFilter(int threshold, Random rng) {
       this(threshold, rng, FPP);
    }

    public SetBackedScalingCuckooFilter(int threshold, Random rng, double fpp) {
        this.hashes = new HashSet<>(threshold);
        this.threshold = threshold;
        this.rng = rng;
        this.capacity = FILTER_CAPACITY;
        this.fpp = fpp;
    }

    public SetBackedScalingCuckooFilter(StreamInput in, Random rng) throws IOException {
        this.threshold = in.readVInt();
        this.isSetMode = in.readBoolean();
        this.rng = rng;
        this.capacity = in.readVInt();
        this.fpp = in.readDouble();

        if (isSetMode) {
            this.hashes = in.readSet(in1 -> {
                MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                hash.h1 = in1.readZLong();
                hash.h2 = in1.readZLong();
                return hash;
            });
        } else {
            this.filters = in.readList(CuckooFilter::new);
        }
    }

    public SetBackedScalingCuckooFilter(SetBackedScalingCuckooFilter other) {
        this.threshold = other.threshold;
        this.isSetMode = other.isSetMode;
        this.rng = other.rng;
        this.breaker = other.breaker;
        this.capacity = other.capacity;
        this.fpp = other.fpp;
        if (isSetMode) {
            this.hashes = new HashSet<>(other.hashes);
        } else {
            this.filters = new ArrayList<>(other.filters);
        }
    }

    public void registerBreaker(Consumer<Long> breaker) {
        this.breaker = breaker;
        breaker.accept(getSizeInBytes());
    }

    public boolean mightContain(BytesRef value) {
        return mightContain(value.bytes, value.offset, value.length);
    }

    public boolean mightContain(byte[] value) {
        return mightContain(value, 0, value.length);
    }

    public boolean mightContain(long value) {
        return mightContain(Numbers.longToBytes(value));
    }

    public boolean mightContain(byte[] bytes, int offset, int length) {
        return mightContain(MurmurHash3.hash128(bytes, offset, length, 0, new MurmurHash3.Hash128()));
    }

    private boolean mightContain(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            return hashes.contains(hash);
        }
        return filters.stream().anyMatch(filter -> filter.mightContain(hash));
    }

    public boolean add(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());
        return add(hash);
    }

    public boolean add(byte[] value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value, 0, value.length, 0, new MurmurHash3.Hash128());
        return add(hash);
    }

    public boolean add(long value) {
        return add(Numbers.longToBytes(value));
    }

    private boolean add(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            hashes.add(hash);
            if (hashes.size() > threshold) {
                convert();
            }
            return true;
        }

        boolean success = filters.get(filters.size() - 1).add(hash);
        if (success == false) {
            // filter is full, create a new one and insert there
            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
            t.add(hash);
            filters.add(t);
            breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter
        }
        return true;
    }

    private void convert() {
        if (isSetMode) {
            long oldSize = getSizeInBytes();

            filters = new ArrayList<>();
            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
            hashes.forEach(t::add);
            filters.add(t);

            hashes = null;
            isSetMode = false;

            breaker.accept(-oldSize); // this zeros out the overhead of the set
            breaker.accept(getSizeInBytes()); // this adds back in the new overhead of the cuckoo filters
        }
    }

    /**
     * Get the approximate size of this datastructure.  Approximate because only the Set occupants
     * are tracked, not the overhead of the Set itself.
     */
    public long getSizeInBytes() {
        long bytes = 0;
        if (hashes != null) {
            bytes = (hashes.size() * 16) + 8 + 4 + 1;
        }
        if (filters != null) {
            bytes += filters.stream().mapToLong(CuckooFilter::getSizeInBytes).sum();
        }
        return bytes;
    }

    public long filterCount() {
        // TODO remove this just for debug
        return filters.size();
    }

    public String getDetails() {
        // TODO remove this just for debug
        return filters.get(0).getDetails();
    }

    /**
     * Merge `other` cuckoo filter into this cuckoo.  After merging, this filter's state will
     * be the union of the two.  During the merging process, the internal Set may be upgraded
     * to a cuckoo if it goes over threshold
     *
     * TODO Today, merging simply entails appending the other filter's collection into this collection.
     * In the future, there is opportunity to merge and deduplicate fingerprints, and/or resize on merge rather
     * than keeping a collection
     */
    public void merge(SetBackedScalingCuckooFilter other) {
        if (isSetMode && other.isSetMode) {
            // Both in sets, merge collections then see if we need to convert to cuckoo
            hashes.addAll(other.hashes);
            if (hashes.size() > threshold) {
                convert();
            }
        } else if (isSetMode && other.isSetMode == false) {
            // Other is in cuckoo mode, so we convert our set to a cuckoo then merge collections.
            // We could probably get fancy and keep our side in set-mode, but simpler to just convert
            convert();
            filters.addAll(other.filters);
        } else if (isSetMode == false && other.isSetMode) {
            // Rather than converting the other to a cuckoo first, we can just
            // replay the values directly into our filter.
            other.hashes.forEach(this::add);
        } else {
            // Both are in cuckoo mode, merge raw fingerprints
            //filters.addAll(other.filters);



            int current = 0;
            CuckooFilter currentFilter = filters.get(current);
            for (CuckooFilter filter : other.filters) {

                Iterator<long[]> iter = filter.getBuckets();
                int bucket = 0;
                while (iter.hasNext()) {
                    long[] fingerprints = iter.next();
                    for (long fingerprint : fingerprints) {
                        if (fingerprint == CuckooFilter.EMPTY || mightContainFingerprint(bucket, (int) fingerprint)) {
                            continue;
                        }
                        boolean success = false;
                        while (success == false) {
                            success = currentFilter.mergeFingerprint(bucket, (int) fingerprint);

                            // If we failed to insert, the current filter is full, get next one
                            if (success == false) {
                                current += 1;

                                // if we're out of filters, we need to create a new one
                                if (current >= filters.size()) {
                                    CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
                                    filters.add(t);
                                    breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter

                                }
                                currentFilter = filters.get(current);
                            }
                        }
                    }
                    bucket += 1;
                }
            }

        }
    }

    private boolean mightContainFingerprint(int bucket, int fingerprint) {
        return filters.stream().anyMatch(filter -> filter.mightContainFingerprint(bucket, fingerprint));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(threshold);
        out.writeBoolean(isSetMode);
        out.writeVInt(capacity);
        out.writeDouble(fpp);
        if (isSetMode) {
            out.writeCollection(hashes, (out1, hash) -> {
                out1.writeZLong(hash.h1);
                out1.writeZLong(hash.h2);
            });
        } else {
            out.writeList(filters);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashes, filters, threshold, isSetMode, capacity, fpp);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SetBackedScalingCuckooFilter that = (SetBackedScalingCuckooFilter) other;
        return Objects.equals(this.hashes, that.hashes)
            && Objects.equals(this.filters, that.filters)
            && Objects.equals(this.threshold, that.threshold)
            && Objects.equals(this.isSetMode, that.isSetMode)
            && Objects.equals(this.capacity, that.capacity)
            && Objects.equals(this.fpp, that.fpp);
    }
}
