package org.elasticsearch.search.aggregations.bucket.multi.range.ip4v;

import org.elasticsearch.search.aggregations.bucket.multi.range.RangeBuilderBase;
import org.elasticsearch.search.aggregations.bucket.multi.range.date.InternalDateRange;

/**
 *
 */
public class IPv4RangeBuilder extends RangeBuilderBase<IPv4RangeBuilder> {

    public IPv4RangeBuilder(String name) {
        super(name, InternalIPv4Range.TYPE.name());
    }

    public IPv4RangeBuilder addRange(String key, String from, String to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public IPv4RangeBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    public IPv4RangeBuilder addUnboundedTo(String key, String to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public IPv4RangeBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    public IPv4RangeBuilder addUnboundedFrom(String key, String from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public IPv4RangeBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

}
