/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.l;

public class ConcatProcessorTests extends AbstractWireSerializingTestCase<ConcatFunctionProcessor> {
    
    @Override
    protected ConcatFunctionProcessor createTestInstance() {
        return new ConcatFunctionProcessor(
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)), 
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)));
    }

    @Override
    protected Reader<ConcatFunctionProcessor> instanceReader() {
        return ConcatFunctionProcessor::new;
    }
    
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }
    
    public void testConcatFunctionWithValidInput() {
        assertEquals("foobar", new Concat(EMPTY, l("foo"), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("fb", new Concat(EMPTY, l('f'), l('b')).makeProcessorDefinition().asProcessor().process(null));
    }
    
    public void testConcatFunctionWithEdgeCases() {
        assertEquals("foo", new Concat(EMPTY, l("foo"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("bar", new Concat(EMPTY, l(null), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Concat(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
    }
    
    public void testConcatFunctionInputsValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Concat(EMPTY, l(5), l("foo")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Concat(EMPTY, l("foo bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae.getMessage());
    }
}
