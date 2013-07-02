package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.ValuesSourceBucketAggregationBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class TermsBuilder extends ValuesSourceBucketAggregationBuilder<TermsBuilder> {

    private int size = -1;
    private Terms.ValueType valueType;

    public TermsBuilder(String name) {
        super(name, "terms");
    }

    public TermsBuilder size(int size) {
        this.size = size;
        return this;
    }

    public TermsBuilder valueType(Terms.ValueType valueType) {
        this.valueType = valueType;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (size >=0) {
            builder.field("size", size);
        }
        if (valueType != null) {
            builder.field("value_type", valueType.name().toLowerCase(Locale.ROOT));
        }
        return builder;
    }
}
