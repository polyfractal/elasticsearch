package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.Writeable;

public abstract class ApproximateSetMembership implements Writeable {
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

    public boolean addUnchecked(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());
        return addUnchecked(hash);
    }

    public boolean addUnchecked(byte[] value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value, 0, value.length, 0, new MurmurHash3.Hash128());
        return addUnchecked(hash);
    }

    public boolean addUnchecked(long value) {
        return addUnchecked(Numbers.longToBytes(value));
    }

    abstract boolean mightContain(MurmurHash3.Hash128 hash);
    abstract boolean add(MurmurHash3.Hash128 hash);
    abstract boolean addUnchecked(MurmurHash3.Hash128 hash);
}
