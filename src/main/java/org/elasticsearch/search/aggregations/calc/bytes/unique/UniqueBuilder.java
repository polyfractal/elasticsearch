package org.elasticsearch.search.aggregations.calc.bytes.unique;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.calc.CalcAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class UniqueBuilder extends CalcAggregationBuilder<UniqueBuilder> {

    private String field;

    public UniqueBuilder(String name) {
        super(name, InternalUnique.TYPE.name());
    }

    public UniqueBuilder field(String field) {
        this.field = field;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }
    }
}
