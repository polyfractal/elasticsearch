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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a list initialization shortcut.
 */
public final class EListInit extends AExpression {
    private final List<AExpression> values;

    private PainlessConstructor constructor = null;
    private PainlessMethod method = null;

    public EListInit(Location location, List<AExpression> values) {
        super(location);

        this.values = values;
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        if (input.read == false) {
            throw createError(new IllegalArgumentException("Must read from list initializer."));
        }

        output.actual = ArrayList.class;

        constructor = scriptRoot.getPainlessLookup().lookupPainlessConstructor(output.actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/0] not found"));
        }

        method = scriptRoot.getPainlessLookup().lookupPainlessMethod(output.actual, false, "add", 1);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(output.actual) + ", add/1] not found"));
        }

        for (int index = 0; index < values.size(); ++index) {
            AExpression expression = values.get(index);

            Input expressionInput = new Input();
            expressionInput.expected = def.class;
            expressionInput.internal = true;
            expression.analyze(scriptRoot, scope, expressionInput);
            expression.cast();
        }

        return output;
    }

    @Override
    ListInitializationNode write(ClassNode classNode) {
        ListInitializationNode listInitializationNode = new ListInitializationNode();

        for (AExpression value : values) {
            listInitializationNode.addArgumentNode(value.cast(value.write(classNode)));
        }

        listInitializationNode.setLocation(location);
        listInitializationNode.setExpressionType(output.actual);
        listInitializationNode.setConstructor(constructor);
        listInitializationNode.setMethod(method);

        return listInitializationNode;
    }

    @Override
    public String toString() {
        return singleLineToString(values);
    }
}
