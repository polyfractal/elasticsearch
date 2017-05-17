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

package org.elasticsearch.search.aggregations.pipeline.interpolate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class InterpolatePipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;


    public InterpolatePipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, GapPolicy gapPolicy,
                                         Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    public InterpolatePipelineAggregator(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return InterpolatePipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalBucket>
            histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
            InternalBucket>) aggregation;
        List<? extends InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();

        for (int i = 0; i < buckets.size(); i++) {
            InternalBucket bucket = buckets.get(i);

            String bucketsPath = bucketsPaths()[0];
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPath, GapPolicy.NONE);

            // Default is to reuse existing bucket.  Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            Bucket newBucket = bucket;

            if ((thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {
                List<InternalBucket> emptyBuckets = new ArrayList<>(10);

                int nextNonEmpty = -1;
                for (int j = i+1; j < buckets.size(); j++) {
                    Double value = resolveBucketValue(histo, buckets.get(j), bucketsPath, GapPolicy.NONE);
                    if (!(value == null || value.isNaN())) {
                        nextNonEmpty = j;
                        break;
                    }
                    emptyBuckets.add(bucket);
                }

                List<InternalBucket> interpolatedBuckets = interpolateBuckets(histo, emptyBuckets, bucketsPath,
                    i-1, nextNonEmpty, gapPolicy, formatter, name(), metaData(), factory);
                newBuckets.addAll(interpolatedBuckets);
                i = nextNonEmpty;
            } else {
                newBuckets.add(newBucket);
            }

        }


        return factory.createAggregation(newBuckets);
    }

    private static List<InternalBucket> interpolateBuckets(MultiBucketsAggregation histo,
                                                   List<InternalBucket> buckets,
                                                   String bucketsPath, int firstNonEmpty, int nextNonEmpty,
                                                   GapPolicy gapPolicy, DocValueFormat formatter, String name,
                                                   Map<String, Object> metadata, HistogramFactory factory) {
        if (gapPolicy.equals(GapPolicy.AVERAGE)) {
            return interpolateAverage(histo, buckets, bucketsPath, firstNonEmpty,
                nextNonEmpty, formatter, name, metadata, factory);
        }

        return buckets;
    }

    private static List<InternalBucket> interpolateAverage(MultiBucketsAggregation histo,
                                                   List<InternalBucket> buckets,
                                                   String bucketsPath, int firstNonEmpty, int nextNonEmpty,
                                                   DocValueFormat formatter, String name,
                                                   Map<String, Object> metadata, HistogramFactory factory) {

        // If the series starts or ends with null/NaN, there's nothing to average...
        if (firstNonEmpty == -1 || nextNonEmpty == -1) {
            return buckets.stream().map(b -> createBucket(b, Double.NaN, formatter, name, metadata, factory))
                .collect(Collectors.toList());
        }

        //if the first value is null/NaN, can't average...
        Double firstValue = resolveBucketValue(histo, buckets.get(firstNonEmpty), bucketsPath, GapPolicy.NONE);
        if (firstValue == null || firstValue.isNaN()) {
            return buckets.stream().map(b -> createBucket(b, Double.NaN, formatter, name, metadata, factory))
                .collect(Collectors.toList());
        }

        //if the next non-empty value is null/NaN, can't average...
        Double nextNonEmptyValue = resolveBucketValue(histo, buckets.get(nextNonEmpty), bucketsPath, GapPolicy.NONE);
        if (nextNonEmptyValue == null || nextNonEmptyValue.isNaN()) {
            return buckets.stream().map(b -> createBucket(b, Double.NaN, formatter, name, metadata, factory))
                .collect(Collectors.toList());
        }

        double avg = (firstValue + nextNonEmptyValue) / (nextNonEmpty - firstNonEmpty);
        return IntStream
            .range(0, buckets.size())
            .mapToObj(i -> createBucket(buckets.get(i), avg * i, formatter, name, metadata, factory))
            .collect(Collectors.toList());

    }

    private static InternalBucket createBucket(Bucket bucket, Double interpolatedValue, DocValueFormat formatter,
                                       String name, Map<String, Object> metadata, HistogramFactory factory) {
        List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
            .map((p) -> (InternalAggregation) p).collect(Collectors.toList());
        aggs.add(new InternalSimpleValue(name, interpolatedValue, formatter, new ArrayList<>(), metadata));
        return (InternalBucket)factory.createBucket(factory.getKey(bucket), bucket.getDocCount(),
            new InternalAggregations(aggs));
    }
}
