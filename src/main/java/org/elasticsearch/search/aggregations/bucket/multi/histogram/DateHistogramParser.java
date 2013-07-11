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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DateHistogramParser implements AggregatorParser {

    private final ImmutableMap<String, DateFieldParser> dateFieldParsers;

    public DateHistogramParser() {
        dateFieldParsers = MapBuilder.<String, DateFieldParser>newMapBuilder()
                .put("year", new DateFieldParser.YearOfCentury())
                .put("1y", new DateFieldParser.YearOfCentury())
                .put("quarter", new DateFieldParser.Quarter())
                .put("1q", new DateFieldParser.Quarter())
                .put("month", new DateFieldParser.MonthOfYear())
                .put("1M", new DateFieldParser.MonthOfYear())
                .put("week", new DateFieldParser.WeekOfWeekyear())
                .put("1w", new DateFieldParser.WeekOfWeekyear())
                .put("day", new DateFieldParser.DayOfMonth())
                .put("1d", new DateFieldParser.DayOfMonth())
                .put("hour", new DateFieldParser.HourOfDay())
                .put("1h", new DateFieldParser.HourOfDay())
                .put("minute", new DateFieldParser.MinuteOfHour())
                .put("1m", new DateFieldParser.MinuteOfHour())
                .put("second", new DateFieldParser.SecondOfMinute())
                .put("1s", new DateFieldParser.SecondOfMinute())
                .immutableMap();
    }

    @Override
    public String type() {
        return InternalDateHistogram.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean multiValued = true;
        boolean keyed = false;
        InternalOrder order = InternalOrder.KEY_ASC;
        String interval = null;
        Chronology chronology = ISOChronology.getInstanceUTC();
        boolean preZoneAdjustLargeInterval = false;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        String format = null;
        long preOffset = 0;
        long postOffset = 0;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("script_lang".equals(currentFieldName) || "scriptLang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("time_zone".equals(currentFieldName) || "timeZone".equals(currentFieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone".equals(currentFieldName) || "preZone".equals(currentFieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone_adjust_large_interval".equals(currentFieldName) || "preZoneAdjustLargeInterval".equals(currentFieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if ("post_zone".equals(currentFieldName) || "postZone".equals(currentFieldName)) {
                    postZone = parseZone(parser, token);
                } else if ("pre_offset".equals(currentFieldName) || "preOffset".equals(currentFieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if ("post_offset".equals(currentFieldName) || "postOffset".equals(currentFieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if ("interval".equals(currentFieldName)) {
                    interval = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("multi_valued".equals(currentFieldName) || "multiValued".equals(currentFieldName)) {
                    multiValued = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            boolean asc = "asc".equals(dir);
                            order = resolveOrder(currentFieldName, asc);
                            //TODO should we throw an error if the value is not "asc" or "desc"???
                        }
                    }
                }
            }
        }

        InternalDateHistogram.Factory histoFactory = new InternalDateHistogram.Factory();

        if (interval == null) {
            throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateFieldParser fieldParser = dateFieldParsers.get(interval);
        if (fieldParser != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(fieldParser.parse(chronology));
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        TimeZoneRounding rounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .build();

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = new ValueFormatter.DateTime(format);
        }

        if (field == null) {

            if (searchScript != null) {
                return new HistogramAggregator.ScriptFactory(aggregationName, searchScript, multiValued, rounding, order, keyed, formatter, histoFactory);
            }

            // falling back on the get field data context
            return new HistogramAggregator.ContextBasedFactory(aggregationName, rounding, order, keyed, histoFactory);
        }

        FieldMapper mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            return new UnmappedDateHistogramAggregator.Factory(aggregationName, order, keyed);
        }

        if (!(mapper instanceof DateFieldMapper)) {
            throw new SearchParseException(context, "date histogram can only be aggregated on date fields but  [" + field + "] is not a date field");
        }

        IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
        FieldContext fieldContext = new FieldContext(field, indexFieldData, mapper);
        return new HistogramAggregator.FieldDataFactory(aggregationName, fieldContext, searchScript, rounding, order, keyed, formatter, histoFactory);
    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return asc ? HistogramBase.Order.KEY_ASC : HistogramBase.Order.KEY_DESC;
        }
        if ("_count".equals(key)) {
            return asc ? HistogramBase.Order.COUNT_ASC : HistogramBase.Order.COUNT_DESC;
        }
        int i = key.indexOf('.');
        if (i < 0) {
            return HistogramBase.Order.aggregation(key, asc);
        }
        return HistogramBase.Order.aggregation(key.substring(0, i), key.substring(i + 1), asc);
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null).millis();
    }

    private DateTimeZone parseZone(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            String text = parser.text();
            int index = text.indexOf(':');
            if (index != -1) {
                int beginIndex = text.charAt(0) == '+' ? 1 : 0;
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(beginIndex, index)),
                        Integer.parseInt(text.substring(index + 1))
                );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

    static interface DateFieldParser {

        DateTimeField parse(Chronology chronology);

        static class WeekOfWeekyear implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.weekOfWeekyear();
            }
        }

        static class YearOfCentury implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class Quarter implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return Joda.QuarterOfYear.getField(chronology);
            }
        }

        static class MonthOfYear implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }

        static class HourOfDay implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateFieldParser {
            @Override
            public DateTimeField parse(Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }
    }
}
