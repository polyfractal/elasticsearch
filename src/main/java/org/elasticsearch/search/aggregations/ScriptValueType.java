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

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 *
 */
public enum ScriptValueType {

    //TODO should we add date & ip as well???
    BYTE("byte", true, false), SHORT("short", true, false), INTEGER("integer", true, false), LONG("long", true, false),
    FLOAT("float", true, true), DOUBLE("double", true, true),
    STRING("string", false, false);


    private final String name;
    private final boolean numeric;
    private final boolean floatingPoint;

    private ScriptValueType(String name, boolean numeric, boolean floatingPoint) {
        this.name = name;
        this.numeric = numeric;
        this.floatingPoint = floatingPoint;
    }

    public static ScriptValueType resolveByName(String name) {
        if ("byte".equals(name)) {
            return BYTE;
        }
        if ("short".equals(name)) {
            return SHORT;
        }
        if ("integer".equals(name)) {
            return INTEGER;
        }
        if ("long".equals(name)) {
            return LONG;
        }
        if ("float".equals(name)) {
            return FLOAT;
        }
        if ("double".equals(name)) {
            return DOUBLE;
        }
        if ("string".equals(name)) {
            return STRING;
        }
        throw new ElasticSearchIllegalArgumentException("Unknown script value type [" + name + "]");
    }

    public boolean numeric() {
        return numeric;
    }

    public boolean floatingPoint() {
        return floatingPoint;
    }

}
