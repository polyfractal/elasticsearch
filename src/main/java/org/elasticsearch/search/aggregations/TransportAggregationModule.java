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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.date.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.terms.doubles.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.longs.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.string.StringTerms;
import org.elasticsearch.search.aggregations.calc.stats.avg.InternalAvg;
import org.elasticsearch.search.aggregations.calc.stats.count.InternalCount;
import org.elasticsearch.search.aggregations.calc.stats.max.InternalMax;
import org.elasticsearch.search.aggregations.calc.stats.min.InternalMin;
import org.elasticsearch.search.aggregations.calc.stats.stats.InternalExtendedStats;
import org.elasticsearch.search.aggregations.calc.stats.stats.InternalStats;
import org.elasticsearch.search.aggregations.calc.stats.sum.InternalSum;

/**
 * A module that registers all the transport streams for the aggregations
 */
public class TransportAggregationModule extends AbstractModule {

    @Override
    protected void configure() {

        // calcs
        InternalAvg.registerStreams();
        InternalSum.registerStreams();
        InternalMin.registerStreams();
        InternalMax.registerStreams();
        InternalCount.registerStreams();
        InternalStats.registerStreams();
        InternalExtendedStats.registerStreams();

        // buckets
        InternalGlobal.registerStreams();
        InternalFilter.registerStreams();
        InternalMissing.registerStreams();
        StringTerms.registerStreams();
        LongTerms.registerStreams();
        DoubleTerms.registerStreams();
        InternalRange.registerStream();
        InternalHistogram.registerStream();
        InternalDateHistogram.registerStream();
    }
}
