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
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.ScorerAware;

/**
 *
 */
public interface ValuesSource {

    Object key();

    public abstract static class FieldData<FDS extends FieldDataSource> implements ValuesSource {

        protected final FDS source;

        public FieldData(FDS source) {
            this.source = source;
        }

        @Override
        public Object key() {
            return source.field();
        }

    }

    public abstract static class Script<Values extends ScriptValues> implements ValuesSource, ReaderContextAware, ScorerAware {

        protected final Values values;

        public Script(Values values) {
            this.values = values;
        }

        public void setNextReader(AtomicReaderContext reader){
            values.script().setNextReader(reader);
            values.clearCache();
        }

        @Override
        public void setScorer(Scorer scorer) {
            values.script().setScorer(scorer);
        }

        @Override
        public Object key() {
            //TODO is this right??? is toString the right thing to represent the key for the script values?
            return values.script();
        }

    }
}
