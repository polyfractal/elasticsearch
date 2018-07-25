/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * This class is the main wrapper object that is serialized into the PersistentTask's cluster state.
 * It holds the config (RollupJobConfig) and a map of authentication headers.  Only RollupJobConfig
 * is ever serialized to the user, so the headers should never leak
 */
public class RollupJob extends AbstractDiffable<RollupJob> implements XPackPlugin.XPackPersistentTaskParams {

    public static final String NAME = "xpack/rollup/job";

    private final Map<String, String> headers;
    private final RollupJobConfig config;

    // Flag holds the state of the ID scheme, e.g. if it has been upgraded to the
    // concatenation scheme.  See #32372 for more details
    private boolean upgradedDocumentID;

    private static final ParseField CONFIG = new ParseField("config");
    private static final ParseField HEADERS = new ParseField("headers");
    private static final ParseField UPGRADED_DOC_ID = new ParseField("upgraded_doc_id");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupJob, Void> PARSER
            = new ConstructingObjectParser<>(NAME, true, a -> new RollupJob((RollupJobConfig) a[0],
                (Map<String, String>) a[1], (Boolean)a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> RollupJobConfig.PARSER.apply(p,c).build(), CONFIG);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
        // Optional to accommodate old versions of state
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), UPGRADED_DOC_ID);
    }

    public RollupJob(RollupJobConfig config, Map<String, String> headers, Boolean upgradedDocumentID) {
        this.config = Objects.requireNonNull(config);
        this.headers = headers == null ? Collections.emptyMap() : headers;
        this.upgradedDocumentID = upgradedDocumentID != null ? upgradedDocumentID : false;
    }

    public RollupJob(StreamInput in) throws IOException {
        this.config = new RollupJobConfig(in);
        headers = in.readMap(StreamInput::readString, StreamInput::readString);
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            upgradedDocumentID = in.readBoolean();
        } else {
            // If we're getting this job from a pre-6.4.0 node,
            // it is using the old ID scheme
            upgradedDocumentID = false;
        }
    }

    public RollupJobConfig getConfig() {
        return config;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public boolean isUpgradedDocumentID() {
        return upgradedDocumentID;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG.getPreferredName(), config);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.field(UPGRADED_DOC_ID.getPreferredName(), upgradedDocumentID);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeBoolean(upgradedDocumentID);
        }
    }

    static Diff<RollupJob> readJobDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(RollupJob::new, in);
    }

    public static RollupJob fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollupJob that = (RollupJob) other;

        return Objects.equals(this.config, that.config)
                && Objects.equals(this.headers, that.headers)
                && Objects.equals(this.upgradedDocumentID, that.upgradedDocumentID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, headers, upgradedDocumentID);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_3_0;
    }
}
