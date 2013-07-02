package org.elasticsearch.search.aggregations.bucket.multi.range.date;

import org.elasticsearch.search.aggregations.bucket.multi.range.RangeBuilderBase;

/**
 *
 */
public class DateRangeBuilder extends RangeBuilderBase<DateRangeBuilder> {

    public DateRangeBuilder(String name) {
        super(name, InternalDateRange.TYPE.name());
    }

    public DateRangeBuilder addRange(String key, Object from, Object to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public DateRangeBuilder addRange(Object from, Object to) {
        return addRange(null, from, to);
    }

    public DateRangeBuilder addUnboundedTo(String key, Object to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public DateRangeBuilder addUnboundedTo(Object to) {
        return addUnboundedTo(null, to);
    }

    public DateRangeBuilder addUnboundedFrom(String key, Object from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public DateRangeBuilder addUnboundedFrom(Object from) {
        return addUnboundedFrom(null, from);
    }

}
