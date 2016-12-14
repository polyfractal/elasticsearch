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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class RareTermsAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, RareTermsAggregationBuilder> {
    public static final String NAME = "rare_terms";
    private static final InternalAggregation.Type TYPE = new Type("rare_terms");

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField MAX_DOC_COUNT_FIELD_NAME = new ParseField("max_doc_count");

    static final BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new BucketCountThresholds(1, 0, 10,
        -1);
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");

    private static final ObjectParser<RareTermsAggregationBuilder, QueryParseContext> PARSER;
    static {
        PARSER = new ObjectParser<>(RareTermsAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true);

        PARSER.declareBoolean(RareTermsAggregationBuilder::showTermDocCountError,
            RareTermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

        PARSER.declareInt(RareTermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(RareTermsAggregationBuilder::maxDocCount, MAX_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(RareTermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(RareTermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(RareTermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);

        PARSER.declareField(RareTermsAggregationBuilder::collectMode,
            (p, c) -> SubAggCollectionMode.parse(p.text(), c.getParseFieldMatcher()),
            SubAggCollectionMode.KEY, ObjectParser.ValueType.STRING);

        PARSER.declareObjectArray(RareTermsAggregationBuilder::order, RareTermsAggregationBuilder::parseOrderParam,
            RareTermsAggregationBuilder.ORDER_FIELD);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
            IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
            IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    public static AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return PARSER.parse(context.parser(), new RareTermsAggregationBuilder(aggregationName, null), context);
    }

    private Order order = Terms.Order.compound(Terms.Order.count(false), Terms.Order.term(true));
    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private SubAggCollectionMode collectMode = null;
    private BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private boolean showTermDocCountError = false;
    private long maxDocCount = 1;

    public RareTermsAggregationBuilder(String name, ValueType valueType) {
        super(name, TYPE, ValuesSourceType.ANY, valueType);
    }

    /**
     * Read from a stream.
     */
    public RareTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in, TYPE, ValuesSourceType.ANY);
        bucketCountThresholds = new BucketCountThresholds(in);
        collectMode = in.readOptionalWriteable(SubAggCollectionMode::readFromStream);
        executionHint = in.readOptionalString();
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        order = InternalOrder.Streams.readOrder(in);
        showTermDocCountError = in.readBoolean();
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalWriteable(collectMode);
        out.writeOptionalString(executionHint);
        out.writeOptionalWriteable(includeExclude);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeBoolean(showTermDocCountError);
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public RareTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public RareTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                "[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the maximum document count terms should have in order to appear in
     * the response.
     */
    public RareTermsAggregationBuilder maxDocCount(long maxDocCount) {
        if (maxDocCount < 0) {
            throw new IllegalArgumentException(
                "[maxDocCount] must be greater than or equal to 0. Found [" + maxDocCount + "] in [" + name + "]");
        }
        this.maxDocCount = maxDocCount;
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public RareTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned.
     */
    public RareTermsAggregationBuilder order(Terms.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = order;
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned.
     */
    public RareTermsAggregationBuilder order(List<Terms.Order> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        order(Terms.Order.compound(orders));
        return this;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public Terms.Order order() {
        return order;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public RareTermsAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: gets an execution hint to the aggregation.
     */
    public String executionHint() {
        return executionHint;
    }

    /**
     * Expert: set the collection mode.
     */
    public RareTermsAggregationBuilder collectMode(SubAggCollectionMode collectMode) {
        if (collectMode == null) {
            throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
        }
        this.collectMode = collectMode;
        return this;
    }

    /**
     * Expert: get the collection mode.
     */
    public SubAggCollectionMode collectMode() {
        return collectMode;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public RareTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    /**
     * Get whether doc count error will be return for individual terms
     */
    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    /**
     * Set whether doc count error will be return for individual terms
     */
    public RareTermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(SearchContext context, ValuesSourceConfig<ValuesSource> config,
                                                                        AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new RareTermsAggregatorFactory(name, type, config, order, includeExclude, executionHint,
            bucketCountThresholds, showTermDocCountError, maxDocCount, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        if (executionHint != null) {
            builder.field(RareTermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (collectMode != null) {
            builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(bucketCountThresholds, collectMode, executionHint, includeExclude, order, showTermDocCountError);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        RareTermsAggregationBuilder other = (RareTermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
            && Objects.equals(collectMode, other.collectMode)
            && Objects.equals(executionHint, other.executionHint)
            && Objects.equals(includeExclude, other.includeExclude)
            && Objects.equals(order, other.order)
            && Objects.equals(showTermDocCountError, other.showTermDocCountError)
            && Objects.equals(maxDocCount, other.maxDocCount);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static Terms.Order parseOrderParam(XContentParser parser, QueryParseContext context) throws IOException {
        XContentParser.Token token;
        Terms.Order orderParam = null;
        String orderKey = null;
        boolean orderAsc = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                orderKey = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                String dir = parser.text();
                if ("asc".equalsIgnoreCase(dir)) {
                    orderAsc = true;
                } else if ("desc".equalsIgnoreCase(dir)) {
                    orderAsc = false;
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown terms order direction [" + dir + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "Unexpected token " + token + " for [order]");
            }
        }
        if (orderKey == null) {
            throw new ParsingException(parser.getTokenLocation(),
                "Must specify at least one field for [order]");
        } else {
            orderParam = resolveOrder(orderKey, orderAsc);
        }
        return orderParam;
    }

    static Terms.Order resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return Order.term(asc);
        }
        if ("_count".equals(key)) {
            return Order.count(asc);
        }
        return Order.aggregation(key, asc);
    }
}
