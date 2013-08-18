package org.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A base class for all bucket aggregation builders.
 */
public abstract class BucketAggregationBuilder<B extends BucketAggregationBuilder> extends AggregationBuilder {

    private List<AggregationBuilder> aggregations;
    private BytesReference aggregationsBinary;

    protected BucketAggregationBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Add a sub get to this bucket get.
     */
    public B subAggregation(AggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = Lists.newArrayList();
        }
        aggregations.add(aggregation);
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    public B subAggregation(byte[] aggregationsBinary) {
        return subAggregation(aggregationsBinary, 0, aggregationsBinary.length);
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    public B subAggregation(byte[] aggregationsBinary, int aggregationsBinaryOffset, int aggregationsBinaryLength) {
        return subAggregation(new BytesArray(aggregationsBinary, aggregationsBinaryOffset, aggregationsBinaryLength));
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    public B subAggregation(BytesReference aggregationsBinary) {
        this.aggregationsBinary = aggregationsBinary;
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    public B subAggregation(XContentBuilder facets) {
        return subAggregation(facets.bytes());
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    public B subAggregation(Map facets) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(facets);
            return subAggregation(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + facets + "]", e);
        }
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        builder.field(type);
        internalXContent(builder, params);

        if (aggregations != null || aggregationsBinary != null) {
            builder.startObject("aggregations");

            if (aggregations != null) {
                for (AggregationBuilder subAgg : aggregations) {
                    subAgg.toXContent(builder, params);
                }
            }

            if (aggregationsBinary != null) {
                if (XContentFactory.xContentType(aggregationsBinary) == builder.contentType()) {
                    builder.rawField("aggregations", aggregationsBinary);
                } else {
                    builder.field("aggregations_binary", aggregationsBinary);
                }
            }

            builder.endObject();
        }

        return builder.endObject();
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;
}
