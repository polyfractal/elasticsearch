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

package org.elasticsearch.search.aggregations.bucket.geo.distance;

import com.google.common.collect.Lists;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class GeoDistanceParser implements AggregatorParser {

    @Override
    public String type() {
        return InternalGeoDistance.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        List<String> fields = null;
        List<GeoDistanceAggregator.DistanceRange> ranges = null;
        GeoPoint origin = null;
        DistanceUnit unit = DistanceUnit.KILOMETERS;
        GeoDistance distanceType = GeoDistance.ARC;

        boolean fieldExists = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    field = parser.text();
                } else if ("unit".equals(currentFieldName)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if ("distance_type".equals(currentFieldName) || "distanceType".equals(currentFieldName)) {
                    distanceType = GeoDistance.fromString(parser.text());
                } else if ("point".equals(currentFieldName) || "origin".equals(currentFieldName)) {
                    origin = new GeoPoint();
                    origin.resetFromString(parser.text());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    fields = new ArrayList<String>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                } else if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<GeoDistanceAggregator.DistanceRange>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        double to = Double.POSITIVE_INFINITY;
                        String key = null;
                        String toOrFromOrKey = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                toOrFromOrKey = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("from".equals(toOrFromOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    to = parser.doubleValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                }
                            }
                        }
                        ranges.add(new GeoDistanceAggregator.DistanceRange(key, from, to));
                    }
                } else if ("point".equals(currentFieldName) || "origin".equals(currentFieldName)) {
                    double lat = Double.NaN;
                    double lon = Double.NaN;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (Double.isNaN(lon)) {
                            lon = parser.doubleValue();
                        } else if (Double.isNaN(lat)) {
                            lat = parser.doubleValue();
                        } else {
                            throw new SearchParseException(context, "malformed [origin] geo point array in geo_distance aggregator [" + aggregationName + "]. a geo point array must be of the form [lon, lat]");
                        }
                    }
                    origin = new GeoPoint(lat, lon);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("point".equals(currentFieldName) || "origin".equals(currentFieldName)) {
                    double lat = Double.NaN;
                    double lon = Double.NaN;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("lat".equals(currentFieldName)) {
                                lat = parser.doubleValue();
                            } else if ("lon".equals(currentFieldName)) {
                                lon = parser.doubleValue();
                            }
                        }
                    }
                    if (Double.isNaN(lat) || Double.isNaN(lon)) {
                        throw new SearchParseException(context, "malformed [origin] geo point object. either [lat] or [lon] (or both) are missing in geo_distance aggregator [" + aggregationName + "]");
                    }
                    origin = new GeoPoint(lat, lon);
                }
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in geo_distance aggregator [" + aggregationName + "]");
        }

        if (origin == null) {
            throw new SearchParseException(context, "Missing [origin] in geo_distance aggregator [" + aggregationName + "]");
        }

        for (GeoDistanceAggregator.DistanceRange range : ranges) {
            range.unit = unit;
            range.origin = origin;
            range.distanceType = distanceType;
        }

        if (!fieldExists) {
            // "field" doesn't exist, so we fall back to the context of the ancestors
            return new GeoDistanceAggregator.Factory(aggregationName, ranges, null);
        }

        if (field != null) {
            FieldMapper mapper = context.smartNameFieldMapper(field);
            if (mapper == null) {
                return new UnmappedGeoDistanceAggregator.Factory(aggregationName, ranges);
            }
            IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
            FieldDataContext fieldDataContext = new FieldDataContext(field, indexFieldData, context);
            return new GeoDistanceAggregator.Factory(aggregationName, ranges, fieldDataContext);
        }

        // fields is specified by the user

        if (fields.isEmpty()) {
            // the user specified an empty array... so we're falling back to the field context of the ancestors
            //TODO what do we do if the script is defined and the user defined an empty array of fields?
            return new GeoDistanceAggregator.Factory(aggregationName, ranges, null);
        }

        List<String> mappedFields = Lists.newArrayListWithCapacity(4);
        List<IndexFieldData> indexFieldDatas = Lists.newArrayListWithCapacity(4);
        for (String fieldName : fields) {
            FieldMapper mapper = context.smartNameFieldMapper(fieldName);
            if (mapper != null) {
                mappedFields.add(fieldName);
                indexFieldDatas.add(context.fieldData().getForField(mapper));
            }
        }

        if (mappedFields.isEmpty()) {
            return new UnmappedGeoDistanceAggregator.Factory(aggregationName, ranges);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(mappedFields, indexFieldDatas, context);
        return new GeoDistanceAggregator.Factory(aggregationName, ranges, fieldDataContext);
    }
}
