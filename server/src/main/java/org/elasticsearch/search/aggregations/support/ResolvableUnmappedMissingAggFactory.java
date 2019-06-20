package org.elasticsearch.search.aggregations.support;

public interface ResolvableUnmappedMissingAggFactory {
    ValuesSource resolveUnmappedMissingVS(Object missingValue);
}
