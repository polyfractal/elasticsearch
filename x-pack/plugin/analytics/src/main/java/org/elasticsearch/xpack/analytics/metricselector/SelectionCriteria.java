/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.metricselector;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum SelectionCriteria implements Writeable {
    MIN {
        @Override
        public double select(double a, double b) {
            return Math.min(a, b) ;
        }
    },
    MAX {
        @Override
        public double select(double a, double b) {
            return Math.max(a, b);
        }
    };

    public static final ParseField SELECTION_CRITERIA = new ParseField("selection_criteria");

    public static SelectionCriteria fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static SelectionCriteria fromStream(StreamInput in) throws IOException {
        return in.readEnum(SelectionCriteria.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        SelectionCriteria selectionCriteria = this;
        out.writeEnum(selectionCriteria);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    public double select(double a, double b) {
        throw new UnsupportedOperationException("Unsupported selection mode");
    }
}
