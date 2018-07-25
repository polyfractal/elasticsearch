/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

public abstract class RollupIDGenerator {
    public abstract void add(Integer v);
    public abstract void add(Long v);
    public abstract void add(Double v);
    public abstract void add(String v);
    public abstract String getID();

    private boolean generated = false;

    final void check(Object v) {
        if (v == null) {
            throw new IllegalArgumentException("ID components cannot be null");
        }
        if (generated) {
            throw new RuntimeException("Cannot update ID as it has already been generated.");
        }
    }

    final void setFlag() {
        if (generated) {
            throw new RuntimeException("Cannot generate ID as it has already been generated.");
        }
        generated = true;
    }

    public static class CRC extends RollupIDGenerator {
        private final CRC32 crc = new CRC32();

        @Override
        public void add(Integer v) {
            check(v);
            crc.update(v);
        }

        @Override
        public void add(Long v) {
            check(v);
            crc.update(Numbers.longToBytes(v), 0, 8);
        }

        @Override
        public void add(Double v) {
            check(v);
            crc.update(Numbers.doubleToBytes(v), 0, 8);
        }

        @Override
        public void add(String v) {
            check(v);
            byte[] vs = (v).getBytes(StandardCharsets.UTF_8);
            crc.update(vs, 0, vs.length);
        }

        @Override
        public String getID() {
            setFlag();
            return String.valueOf(crc.getValue());
        }
    }

    public static class Concat extends RollupIDGenerator {
        private final StringBuilder id = new StringBuilder();

        @Override
        public void add(Integer v) {
            check(v);
            id.append(v);
        }

        @Override
        public void add(Long v) {
            check(v);
            id.append(v);
        }

        @Override
        public void add(Double v) {
            check(v);
            id.append(v);
        }

        @Override
        public void add(String v) {
            check(v);
            id.append(v);
        }

        @Override
        public String getID() {
            setFlag();
            // If the ID is longer than the Lucene limit, we need to hash it
            if (id.length() > 32000) {
                byte[] bytes = id.toString().getBytes(StandardCharsets.UTF_8);
                MurmurHash3.Hash128 hasher = MurmurHash3.hash128(bytes, 0, bytes.length, 19, new MurmurHash3.Hash128());
                byte[] hashedBytes = new byte[16];
                System.arraycopy(Numbers.longToBytes(hasher.h1), 0, hashedBytes, 0, 8);
                System.arraycopy(Numbers.longToBytes(hasher.h1), 0, hashedBytes, 8, 8);
                return Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes);
            }
            // Otherwise just return the string itself
            return id.toString();
        }
    }
}
