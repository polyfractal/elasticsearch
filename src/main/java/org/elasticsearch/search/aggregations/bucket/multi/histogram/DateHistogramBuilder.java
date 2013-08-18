package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.ValuesSourceBucketAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 *
 */
public class DateHistogramBuilder extends ValuesSourceBucketAggregationBuilder<DateHistogramBuilder> {

    private Object interval;
    private HistogramBase.Order order;
    private String preZone;
    private String postZone;
    private boolean preZoneAdjustLargeInterval;
    long preOffset = 0;
    long postOffset = 0;
    float factor = 1.0f;

    public DateHistogramBuilder(String name) {
        super(name, InternalDateHistogram.TYPE.name());
    }

    public DateHistogramBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public DateHistogramBuilder interval(DateHistogram.Interval interval) {
        this.interval = interval;
        return this;
    }

    public DateHistogramBuilder order(DateHistogram.Order order) {
        this.order = order;
        return this;
    }

    public DateHistogramBuilder preZone(String preZone) {
        this.preZone = preZone;
        return this;
    }

    public DateHistogramBuilder postZone(String postZone) {
        this.postZone = postZone;
        return this;
    }

    public DateHistogramBuilder preZoneAdjustLargeInterval(boolean preZoneAdjustLargeInterval) {
        this.preZoneAdjustLargeInterval = preZoneAdjustLargeInterval;
        return this;
    }

    public DateHistogramBuilder preOffset(long preOffset) {
        this.preOffset = preOffset;
        return this;
    }

    public DateHistogramBuilder postOffset(long postOffset) {
        this.postOffset = postOffset;
        return this;
    }

    public DateHistogramBuilder factor(float factor) {
        this.factor = factor;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (interval == null) {
            throw new SearchSourceBuilderException("[interval] must be defined for histogram aggregation [" + name + "]");
        }
        builder.field("interval", interval);

        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }

        if (preZone != null) {
            builder.field("pre_zone", preZone);
        }

        if (postZone != null) {
            builder.field("post_zone", postZone);
        }

        if (preZoneAdjustLargeInterval) {
            builder.field("pre_zone_adjust_large_interval", true);
        }

        if (preOffset != 0) {
            builder.field("pre_offset", preOffset);
        }

        if (postOffset != 0) {
            builder.field("post_offset", postOffset);
        }

        if (factor != 1.0f) {
            builder.field("factor", factor);
        }

        return builder;
    }

}
