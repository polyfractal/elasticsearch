package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.bucket.single.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.single.global.GlobalBuilder;
import org.elasticsearch.search.aggregations.bucket.single.missing.MissingBuilder;
import org.elasticsearch.search.aggregations.bucket.single.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.calc.count.CountBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.max.MaxBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.min.MinBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.stats.ExtendedStatsBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.calc.numeric.sum.SumBuilder;

/**
 *
 */
public class AggregationBuilders {

    protected AggregationBuilders() {
    }

    public static CountBuilder count(String name) {
        return new CountBuilder(name);
    }

    public static AvgBuilder avg(String name) {
        return new AvgBuilder(name);
    }

    public static MaxBuilder max(String name) {
        return new MaxBuilder(name);
    }

    public static MinBuilder min(String name) {
        return new MinBuilder(name);
    }

    public static SumBuilder sum(String name) {
        return new SumBuilder(name);
    }

    public static StatsBuilder stats(String name) {
        return new StatsBuilder(name);
    }

    public static ExtendedStatsBuilder extendedStats(String name) {
        return new ExtendedStatsBuilder(name);
    }

    public static FilterAggregationBuilder filter(String name) {
        return new FilterAggregationBuilder(name);
    }

    public static GlobalBuilder global(String name) {
        return new GlobalBuilder(name);
    }

    public static MissingBuilder missing(String name) {
        return new MissingBuilder(name);
    }

    public static NestedBuilder nested(String name) {
        return new NestedBuilder(name);
    }

}
