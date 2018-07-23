/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RollupFeatureSetUsage extends XPackFeatureSet.Usage {

    private final ParseField ACTIVE_JOBS = new ParseField("active_job_stats");
    private final ParseField ALL_JOBS = new ParseField("all_job_stats");

    private final JobUsage activeJobUsage;
    private final JobUsage allJobUsage;

    public RollupFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        activeJobUsage = new JobUsage(in);
        allJobUsage = new JobUsage(in);
    }

    public RollupFeatureSetUsage(boolean available, boolean enabled, JobUsage activeJobUsage, JobUsage allJobUsage) {
        super(XPackField.ROLLUP, available, enabled);
        this.activeJobUsage = activeJobUsage;
        this.allJobUsage = allJobUsage;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (activeJobUsage != null) {
            builder.startObject(ACTIVE_JOBS.getPreferredName());
            activeJobUsage.toXContent(builder, params);
            builder.endObject();
        }
        if (allJobUsage != null) {
            builder.startObject(ALL_JOBS.getPreferredName());
            allJobUsage.toXContent(builder, params);
            builder.endObject();
        }
    }

    public static class JobUsage implements ToXContentFragment, Writeable {

        private final static ParseField NUM_JOBS = new ParseField("count");
        private final static ParseField NUM_ROLLUP_INDICES = new ParseField("rollup_indices");
        private final static ParseField NUM_SOURCE_PATTERNS = new ParseField("source_patterns");
        private final static ParseField STATES = new ParseField("job_states");
        private final static ParseField GROUPS = new ParseField("group_stats");
        private final static ParseField METRICS = new ParseField("metric_stats");
        private final static ParseField FIELDS_PER_JOB = new ParseField("fields_per_job");
        private final static ParseField METRICS_PER_FIELD = new ParseField("metrics_per_field");
        private final static ParseField CALENDAR_INTERVALS = new ParseField("num_calendar_intervals");
        private final static ParseField FIXED_INTERVALS = new ParseField("num_fixed_intervals");
        private final static ParseField INTERVALS = new ParseField("intervals");

        // general
        long numJobs = 0;
        Set<String> numRollupIndices = new HashSet<>();
        Set<String> numSourcePatterns = new HashSet<>();
        Map<String, Long> activeStates = new HashMap<>(5);

        // metrics
        StatsAccumulator fieldsPerJob = new StatsAccumulator();
        StatsAccumulator metricsPerField = new StatsAccumulator();
        Map<String, Long> metricAggStats = new HashMap<>(RollupField.SUPPORTED_METRICS.size());

        // groups
        Map<String, StatsAccumulator> groupAggStats = new HashMap<>(2);
        long calendarIntervals = 0;
        long fixedIntervals = 0;
        Map<String, Long> intervalStats = new HashMap<>();

        public JobUsage(List<RollupJobConfig> jobs) {
            numJobs = jobs.size();

            // Accumulate the stats in the job config
            jobs.forEach(this::processJobConfig);
        }

        public JobUsage(GetRollupJobsAction.Response response) {
            numJobs = response.getJobs().size();

            response.getJobs().forEach(job -> {
                // Accumulate the stats in the job config
                processJobConfig(job.getJob());

                // Accumulate active states
                activeStates.merge(job.getStatus().getIndexerState().value(), 1L, Long::sum);
            });
        }

        private void processJobConfig(RollupJobConfig config) {
            numRollupIndices.add(config.getRollupIndex());
            numSourcePatterns.add(config.getIndexPattern());

            // Accumulate metric statistics
            fieldsPerJob.add(config.getMetricsConfig().size());
            config.getMetricsConfig().forEach(field -> {
                metricsPerField.add(field.getMetrics().size());

                // Increment the individual agg counts (min, max, etc)
                field.getMetrics().forEach(metric -> metricAggStats.merge(metric, 1L, Long::sum));
            });

            // Accumulate grouping stats
            groupAggStats.put(HistogramAggregationBuilder.NAME, new StatsAccumulator());
            groupAggStats.put(TermsAggregationBuilder.NAME, new StatsAccumulator());
            if (config.getGroupConfig().getHisto() != null) {
                groupAggStats.get(HistogramAggregationBuilder.NAME)
                    .add(config.getGroupConfig().getHisto().getAllFields().size());
            }
            if (config.getGroupConfig().getTerms() != null) {
                groupAggStats.get(TermsAggregationBuilder.NAME)
                    .add(config.getGroupConfig().getTerms().getAllFields().size());
            }

            // Date Histo specific stats
            String interval = config.getGroupConfig().getDateHisto().getInterval().toString();
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(interval)) {
                calendarIntervals += 1;
                switch (interval) {
                    case "1y":
                        intervalStats.merge("year", 1L, Long::sum);
                        break;
                    case "1q":
                        intervalStats.merge("quarter", 1L, Long::sum);
                        break;
                    case "1M":
                        intervalStats.merge("month", 1L, Long::sum);
                        break;
                    case "1w":
                        intervalStats.merge("week", 1L, Long::sum);
                        break;
                    case "1d":
                        intervalStats.merge("day", 1L, Long::sum);
                        break;
                    case "1h":
                        intervalStats.merge("hour", 1L, Long::sum);
                        break;
                    case "1m":
                        intervalStats.merge("minute", 1L, Long::sum);
                        break;
                    case "1s":
                        intervalStats.merge("second", 1L, Long::sum);
                        break;
                    default:
                        // Anything else is already in "second", "minute", etc format, add directly
                        intervalStats.merge(interval, 1L, Long::sum);
                }
            } else {
                fixedIntervals += 1;
                TimeValue intervalInMillis = TimeValue.parseTimeValue(interval, "xpack.usage.interval");
                // Fixed time only has second through day, so just check each boundary
                // in sequence
                if (intervalInMillis.seconds() <= 60) {
                    intervalStats.merge("second", 1L, Long::sum);
                } else if (intervalInMillis.minutes() <= 60) {
                    intervalStats.merge("minute", 1L, Long::sum);
                } else if (intervalInMillis.hours() <= 24) {
                    intervalStats.merge("hour", 1L, Long::sum);
                } else {
                    intervalStats.merge("day", 1L, Long::sum);
                }
            }
        }

        JobUsage(StreamInput in) throws IOException {
            numJobs = in.readVLong();
            numRollupIndices = new HashSet<>(Arrays.asList(in.readStringArray()));
            numSourcePatterns = new HashSet<>(Arrays.asList(in.readStringArray()));
            activeStates = in.readMap(StreamInput::readString, StreamInput::readVLong);

            fieldsPerJob = new StatsAccumulator(in);
            metricsPerField = new StatsAccumulator(in);
            metricAggStats = in.readMap(StreamInput::readString, StreamInput::readVLong);

            groupAggStats = in.readMap(StreamInput::readString, StatsAccumulator::new);
            calendarIntervals = in.readVLong();
            fixedIntervals = in.readVLong();
            intervalStats = in.readMap(StreamInput::readString, StreamInput::readVLong);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(numJobs);
            out.writeStringArray(numRollupIndices.toArray(new String[]{}));
            out.writeStringArray(numSourcePatterns.toArray(new String[]{}));
            out.writeMap(activeStates, StreamOutput::writeString, StreamOutput::writeVLong);

            fieldsPerJob.writeTo(out);
            metricsPerField.writeTo(out);
            out.writeMap(metricAggStats, StreamOutput::writeString, StreamOutput::writeVLong);

            out.writeMap(groupAggStats, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
            out.writeVLong(calendarIntervals);
            out.writeVLong(fixedIntervals);
            out.writeMap(intervalStats, StreamOutput::writeString, StreamOutput::writeVLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NUM_JOBS.getPreferredName(), numJobs);
            builder.field(NUM_ROLLUP_INDICES.getPreferredName(), numRollupIndices);
            builder.field(NUM_SOURCE_PATTERNS.getPreferredName(), numSourcePatterns);
            if (activeStates != null) {
                builder.startObject(STATES.getPreferredName());
                {
                    for (Map.Entry<String, Long> entry : activeStates.entrySet()) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                }
                builder.endObject();
            }
            builder.startObject(GROUPS.getPreferredName());
            {
                // Histo and Terms
                for (Map.Entry<String, StatsAccumulator> entry : groupAggStats.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue().asMap());
                }
                // Date Histo
                builder.startObject(DateHistogramAggregationBuilder.NAME);
                {
                    builder.field(CALENDAR_INTERVALS.getPreferredName(), calendarIntervals);
                    builder.field(FIXED_INTERVALS.getPreferredName(), fixedIntervals);
                    builder.startObject(INTERVALS.getPreferredName());
                    {
                        for (Map.Entry<String, Long> interval : intervalStats.entrySet()) {
                            builder.field(interval.getKey(), interval.getValue());
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            builder.startObject(METRICS.getPreferredName());
            {
                builder.field(FIELDS_PER_JOB.getPreferredName(), fieldsPerJob.asMap());
                builder.field(METRICS_PER_FIELD.getPreferredName(), metricsPerField.asMap());
                for (Map.Entry<String, Long> metric : metricAggStats.entrySet()) {
                    builder.field(metric.getKey(), metric.getValue());
                }
            }
            builder.endObject();
            return builder;
        }


    }
}
