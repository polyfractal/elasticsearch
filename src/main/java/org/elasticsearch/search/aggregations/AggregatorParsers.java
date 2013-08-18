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

package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A registry for all the aggregator parser, also servers as the main parser for the aggregations module
 */
public class AggregatorParsers {

    private final ImmutableMap<String, AggregatorParser> parsers;

    /**
     * Constructs the AggregatorParsers out of all the given parsers
     *
     * @param parsers The available aggregator parsers (dynamically injected by the {@link AggregationModule}).
     */
    @Inject
    public AggregatorParsers(Set<AggregatorParser> parsers) {
        MapBuilder<String, AggregatorParser> builder = MapBuilder.newMapBuilder();
        for (AggregatorParser parser : parsers) {
            builder.put(parser.type(), parser);
        }
        this.parsers = builder.immutableMap();
    }

    /**
     * Returns the parser that is registered under the given aggregation type.
     *
     * @param type  The aggregation type
     * @return      The parser associated with the given aggregation type.
     */
    public AggregatorParser parser(String type) {
        return parsers.get(type);
    }

    /**
     * Parses the aggregation request recursively generating aggregator factories in turn.
     *
     * @param parser    The input xcontent that will be parsed.
     * @param context   The search context.
     *
     * @return          The parsed aggregator factories.
     *
     * @throws IOException When parsing fails for unknown reasons.
     */
    public List<Aggregator.Factory> parseAggregators(XContentParser parser, SearchContext context) throws IOException {
        return parseAggregators(parser, context, 0);
    }


    private List<Aggregator.Factory> parseAggregators(XContentParser parser, SearchContext context, int level) throws IOException {
        XContentParser.Token token = null;
        String currentFieldName = null;

        List<Aggregator.Factory> factories = new ArrayList<Aggregator.Factory>();

        String aggregationName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                aggregationName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String aggregatorType = null;
                Aggregator.Factory factory = null;
                List<Aggregator.Factory> subFactories = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("aggregations".equals(currentFieldName) || "aggs".equals(currentFieldName)) {
                            subFactories = parseAggregators(parser, context, level+1);
                        } else {
                            aggregatorType = currentFieldName;
                            AggregatorParser aggregatorParser = parser(aggregatorType);
                            if (aggregatorParser == null) {
                                throw new SearchParseException(context, "Could not find aggregator type [" + currentFieldName + "]");
                            }
                            factory = aggregatorParser.parse(aggregationName, parser, context);
                        }
                    }
                }

                if (factory == null) {
                    // skipping the aggregation
                    continue;
                }

                if (subFactories != null) {
                    if (!(factory instanceof Aggregator.CompoundFactory)) {
                        throw new SearchParseException(context, "Aggregator of type [" + aggregatorType + "] cannot accept sub-aggregations");
                    }
                    Aggregator.CompoundFactory compoundFactory = (Aggregator.CompoundFactory) factory;
                    compoundFactory.set(subFactories);
                }

                if (level == 0) {
                    factory.validate();
                }

                factories.add(factory);
            }
        }

        return factories;
    }

}
