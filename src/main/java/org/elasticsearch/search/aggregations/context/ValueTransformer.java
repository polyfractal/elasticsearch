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

package org.elasticsearch.search.aggregations.context;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 * A transformer that can transform field data values just before they're aggregated.
 */
public interface ValueTransformer {

    /**
     * A transformer that doesn't do any transformation
     */
    public final static ValueTransformer NONE = new None();

    /**
     * A transformer that doesn't do any transformation
     */
    static class None implements ValueTransformer {

        @Override
        public void setNextDocId(int doc) {
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
        }

        @Override
        public void setScorer(Scorer scorer) {
        }

        @Override
        public long transform(long value) {
            return value;
        }

        @Override
        public double transform(double value) {
            return value;
        }

        @Override
        public BytesRef transform(BytesRef value) {
            return value;
        }
    }

    /**
     * A script based transformer enabling the user to define the transformation on the value using the build-in
     * scripting engine support. The value is accessible to the script via the {@code _value} variable.
     */
    static class Script implements ValueTransformer {

        private final SearchScript script;

        public Script(SearchContext context, String script, String lang, Map<String, Object> params) {
            this(context.scriptService().search(context.lookup(), lang, script, params));
        }

        public Script(SearchScript script) {
            this.script = script;
        }

        @Override
        public void setNextDocId(int doc) {
            script.setNextDocId(doc);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            script.setNextReader(context);
        }

        @Override
        public void setScorer(Scorer scorer) {
            script.setScorer(scorer);
        }

        @Override
        public long transform(long value) {
            script.setNextVar("_value", value);
            return script.runAsLong();
        }

        @Override
        public double transform(double value) {
            script.setNextVar("_value", value);
            return script.runAsDouble();
        }

        @Override
        public BytesRef transform(BytesRef value) {
            script.setNextVar("_value", value.utf8ToString());
            return new BytesRef((CharSequence) script.run());
        }
    }

    /**
     * A transformer that is compound out of an ordered list of other transformers which will be apply the transformation
     * in a chain.
     */
    static class Chain implements ValueTransformer {

        private final ValueTransformer[] transformers;

        public Chain(ValueTransformer... transformers) {
            this.transformers = transformers;
        }

        @Override
        public void setNextDocId(int doc) {
            for (ValueTransformer transformer : transformers) {
                transformer.setNextDocId(doc);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            for (ValueTransformer transformer : transformers) {
                transformer.setNextReader(context);
            }
        }

        @Override
        public void setScorer(Scorer scorer) {
            for (ValueTransformer transformer : transformers) {
                transformer.setScorer(scorer);
            }
        }

        @Override
        public long transform(long value) {
            for (ValueTransformer transformer : transformers) {
                value = transformer.transform(value);
            }
            return value;
        }

        @Override
        public double transform(double value) {
            for (ValueTransformer transformer : transformers) {
                value = transformer.transform(value);
            }
            return value;
        }

        @Override
        public BytesRef transform(BytesRef value) {
            for (ValueTransformer transformer : transformers) {
                value = transformer.transform(value);
            }
            return value;
        }
    }

    /**
     * A numeric value only transformer which multiplies the given numeric value by a constant factor.
     */
    static class Factor extends None {

        private final int factor;

        public Factor(int factor) {
            this.factor = factor;
        }

        @Override
        public long transform(long value) {
            return value * factor;
        }

        @Override
        public double transform(double value) {
            return value * factor;
        }

        @Override
        public BytesRef transform(BytesRef value) {
            throw new ElasticSearchException("Cannot factorize string values");
        }
    }

    void setNextDocId(int doc);

    void setNextReader(AtomicReaderContext context);

    void setScorer(Scorer scorer);

    long transform(long value);

    double transform(double value);

    BytesRef transform(BytesRef value);


}
