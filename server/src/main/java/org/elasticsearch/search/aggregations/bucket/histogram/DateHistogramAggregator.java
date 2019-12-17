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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.MaxAggregator;
import org.elasticsearch.search.aggregations.metrics.MinAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 */
class DateHistogramAggregator extends DeferableBucketAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    private final Rounding shardRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongHash bucketOrds;
    private long offset;

    private final Function<byte[], Number> pointReader;
    private final String pointField;

    DateHistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, Rounding shardRounding,
                            long offset, BucketOrder order, boolean keyed, long minDocCount, @Nullable ExtendedBounds extendedBounds,
                            ValuesSourceConfig<?> config, @Nullable ValuesSource.Numeric valuesSource, DocValueFormat formatter,
                            Function<byte[], Number> pointReader, SearchContext aggregationContext, Aggregator parent,
                            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.rounding = rounding;
        this.shardRounding = shardRounding;
        this.offset = offset;
        this.order = InternalOrder.validate(order, this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        this.pointReader = pointReader;
        if (pointReader != null) {
            pointField = config.fieldContext().field();
        } else {
            pointField = null;
        }

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {

        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        tryBKDOptimization(ctx, sub);

        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        // We can use shardRounding here, which is sometimes more efficient
                        // if daylight saving times are involved.
                        long rounded = shardRounding.round(value - offset) + offset;
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        long bucketOrd = bucketOrds.add(rounded);
                        if (bucketOrd < 0) { // already seen
                            bucketOrd = -1 - bucketOrd;
                            collectExistingBucket(sub, doc, bucketOrd);
                        } else {
                            collectBucket(sub, doc, bucketOrd);
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    private void tryBKDOptimization(LeafReaderContext ctx, LeafBucketCollector sub) throws CollectionTerminatedException, IOException {
        final PointValues pointValues = ctx.reader().getPointValues(pointField);
        if (pointValues != null) {
            final Bits liveDocs = ctx.reader().getLiveDocs();

            // TODO check min/maxPackedValue instead? should be faster but could lead to large range if there are adversarial deleted values
            Number segMin = MinAggregator.findLeafMinValue(ctx.reader(), pointField, pointReader);
            Number segMax = MaxAggregator.findLeafMaxValue(ctx.reader(), pointField, pointReader);

            if (segMin != null && segMax != null) {
                long currentRounding = shardRounding.round(segMin.longValue() - offset) + offset;
                final long end = shardRounding.round(segMax.longValue() - offset) + offset;
                final int maxDoc = ctx.reader().maxDoc();

                try {
                    // pre-allocate what our DocIdSetBuilder will use as worst-case estimate
                    addRequestCircuitBreakerBytes(maxDoc / 8);

                    while (currentRounding <= end) {
                        final long nextRounding = shardRounding.nextRoundingValue(currentRounding);

                        final DocIdSetBuilder docs = new DocIdSetBuilder(maxDoc);
                        pointValues.intersect(getVisitor(liveDocs, currentRounding, nextRounding, docs));

                        DocIdSetIterator iter = docs.build().iterator();

                        // Get a bucket ordinal just like non-optimized collection
                        long bucketOrd = bucketOrds.add(currentRounding);
                        if (bucketOrd < 0) { // already seen
                            bucketOrd = -1 - bucketOrd;
                            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                collectExistingBucket(sub, iter.docID(), bucketOrd);
                            }
                        } else {
                            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                collectBucket(sub, iter.docID(), bucketOrd);
                            }
                        }

                        // Move on to the next bucket
                        currentRounding = shardRounding.nextRoundingValue(currentRounding);
                    }
                    // All done!
                    throw new CollectionTerminatedException();

                } catch (CircuitBreakingException e) {
                    // If we tripped the breaker the DocIdSetBuilder is (potentially) too large.
                    // Exit without throwing CollectionTerminatedException so we can fall back to old method
                } finally {
                    // Make sure we account for DocIdSetBuilder deallocation
                    addRequestCircuitBreakerBytes(-maxDoc / 8);
                }
            }

        }

    }

    private PointValues.IntersectVisitor getVisitor(Bits liveDocs, long currentRounding, long nextRounding, DocIdSetBuilder docs) {
        return new PointValues.IntersectVisitor() {

            DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
                adder = docs.grow(count);
            }

            @Override
            public void visit(int docID) {
                if (liveDocs == null || liveDocs.get(docID)) {
                    adder.add(docID);
                }
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (liveDocs == null || liveDocs.get(docID)) {
                    Number value = pointReader.apply(packedValue);
                    if (value.doubleValue() >= currentRounding && value.doubleValue() < nextRounding) {
                        adder.add(docID);
                    }
                }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                Number value = pointReader.apply(packedValue);
                if (value.doubleValue() >= currentRounding && value.doubleValue() < nextRounding) {

                    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        visit(iterator.docID());
                    }
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                Number max = pointReader.apply(maxPackedValue);
                if (max.doubleValue() < currentRounding) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                Number min = pointReader.apply(minPackedValue);
                if (min.doubleValue() > nextRounding) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (min.doubleValue() >= currentRounding && max.doubleValue() < nextRounding) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }



    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        consumeBucketsAndMaybeBreak((int) bucketOrds.size());

        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.size(); i++) {
            buckets.add(new InternalDateHistogram.Bucket(bucketOrds.get(i), bucketDocCount(i), keyed, formatter, bucketAggregations(i)));
        }

        // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
        CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator(this));

        // value source will be null for unmapped fields
        // Important: use `rounding` here, not `shardRounding`
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, Collections.emptyList(), order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
