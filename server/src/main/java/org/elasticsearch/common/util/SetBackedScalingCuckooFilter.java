package org.elasticsearch.common.util;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

public class SetBackedScalingCuckooFilter extends ApproximateSetMembership {

    private static final int FILTER_CAPACITY = 1000000;
    private static final double FPP = 0.001;

    private Set<MurmurHash3.Hash128> hashes;
    private List<CuckooFilter> filters;
    private final int threshold;
    private final Random rng;
    private boolean isSetMode = true;

    public SetBackedScalingCuckooFilter(int threshold, Random rng) {
        this.hashes = new HashSet<>(threshold);
        this.threshold = threshold;
        this.rng = rng;
    }

    public SetBackedScalingCuckooFilter(StreamInput in) throws IOException {
        this.threshold = in.readVInt();
        this.isSetMode = in.readBoolean();
        this.rng = Randomness.get(); // TODO ok to generate this?

        if (isSetMode) {
            this.hashes = in.readSet(in1 -> {
                MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                hash.h1 = in1.readVLong();
                hash.h2 = in1.readVLong();
                return hash;
            });
        } else {
            this.filters = in.readList(CuckooFilter::new);
        }
    }

    @Override
    boolean mightContain(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            return hashes.contains(hash);
        }
        return filters.stream().anyMatch(filter -> filter.mightContain(hash));
    }

    @Override
    boolean add(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            hashes.add(hash);
            if (hashes.size() > threshold) {
                filters = new ArrayList<>();
                CuckooFilter t = new CuckooFilter(FILTER_CAPACITY, FPP, rng);
                hashes.forEach(t::add);
                filters.add(t);

                hashes = null;
                isSetMode = false;
            }
            return true;
        }

        boolean success = filters.get(filters.size() - 1).add(hash);
        if (success == false) {
            // filter is full, create a new one and insert there
            CuckooFilter t = new CuckooFilter(FILTER_CAPACITY, FPP, rng);
            t.add(hash);
            filters.add(t);
        }
        return true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(threshold);
        out.writeBoolean(isSetMode);
        if (isSetMode) {
            out.writeCollection(hashes, (out1, hash) -> {
                out1.writeVLong(hash.h1);
                out1.writeVLong(hash.h2);
            });
        } else {
            out.writeList(filters);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashes, filters, threshold, isSetMode);
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
            && Objects.equals(this.isSetMode, that.isSetMode);
    }
}
