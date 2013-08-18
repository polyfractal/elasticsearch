package org.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A base class for all bucket aggregation builders that are based on values (either script generated or field data values)
 */
public abstract class ValuesSourceBucketAggregationBuilder<B extends ValuesSourceBucketAggregationBuilder<B>> extends BucketAggregationBuilder<B> {

    private String field;
    private String script;
    private String scriptLang;
    private Map<String, Object> params;
    private Boolean multiValued;

    /**
     * Constructs a new builder.
     *
     * @param name  The name of the aggregation.
     * @param type  The type of the aggregation.
     */
    protected ValuesSourceBucketAggregationBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Sets the field from which the values will be extracted.
     *
     * @param field     The name of the field
     * @return          This builder (fluent interface support)
     */
    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    /**
     * Sets the script which generates the values. If the script is configured along with the field (as in {@link #field(String)}), then
     * this script will be treated as a {@code value script}. A <i>value script</i> will be applied on the values that are extracted from
     * the field data (you can refer to that value in the script using the {@code _value} reserved variable). If only the script is configured
     * (and the no field is configured next to it), then the script will be responsible to generate the values that will be aggregated.
     *
     * @param script    The configured script.
     * @return          This builder (fluent interface support)
     */
    public B script(String script) {
        this.script = script;
        return (B) this;
    }

    /**
     * Sets the language of the script (if one is defined).
     * <p/>
     * Also see {@link #script(String)}.
     *
     * @param scriptLang    The language of the script.
     * @return              This builder (fluent interface support)
     */
    public B scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return (B) this;
    }

    /**
     * When configuring a script to generate the aggregated values, elasticsearch will assume that the script can potentially generate/return
     * multiple values. This assumption may have a performance hit, so if you know that the script will always generate a single value, you
     * can set this to {@code false}.
     * <p/>
     * NOTE: <i>value script</i>s only support single value generation (that is, it accepts a single value and returns a single value).
     *<p/>
     * For more information about <i>value scripts</i>, see {@link #script(String)}.
     *
     * @param multiValued   The language of the script.
     * @return              This builder (fluent interface support)
     */
    public B multiValued(boolean multiValued) {
        this.multiValued = multiValued;
        return (B) this;
    }

    /**
     * Sets the value of a parameter that is used in the script (if one is configured).
     *
     * @param name      The name of the parameter.
     * @param value     The value of the parameter.
     * @return          This builder (fluent interface support)
     */
    public B param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return (B) this;
    }

    /**
     * Sets the values of a parameters that are used in the script (if one is configured).
     *
     * @param params    The the parameters.
     * @return          This builder (fluent interface support)
     */
    public B params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.putAll(params);
        return (B) this;
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        if (scriptLang != null) {
            builder.field("script_lang", scriptLang);
        }
        if (multiValued != null) {
            builder.field("multi_valued", multiValued);
        }
        if (this.params != null) {
            builder.field("params").map(this.params);
        }

        doInternalXContent(builder, params);
        return builder.endObject();
    }

    protected abstract XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException;
}
