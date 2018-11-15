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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.countInnerBucket;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RareTermsAggregatorTests extends AggregatorTestCase {

    private static final String LONG_FIELD = "numeric";
    private static final String KEYWORD_FIELD = "keyword";
    private static final String DOUBLE_FIELD = "double";

    private static final List<Long> dataset;
    static {
        List<Long> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add((long) i);
            }
        }
        dataset  = d;
    }

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.STRING
        );
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.NUMERIC
        );
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(DOUBLE_FIELD).maxDocCount(1),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.DOUBLE
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testBothCases(query, dataset,
            aggregation -> aggregation.field(LONG_FIELD).maxDocCount(1),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(1L));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }, ValueType.NUMERIC
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKeyAsString(), equalTo("1"));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }, ValueType.STRING
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(DOUBLE_FIELD).maxDocCount(1),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                DoubleTerms.Bucket bucket = (DoubleTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(1.0));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }, ValueType.DOUBLE
        );
    }

    public void testIncludeExclude() throws IOException {
        Query query = new MatchAllDocsQuery();

        testBothCases(query, dataset,
            aggregation -> aggregation.field(LONG_FIELD)
                .maxDocCount(2) // bump to 2 since we're only including "2"
                .includeExclude(new IncludeExclude(new long[]{2}, new long[]{})),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(2L));
                assertThat(bucket.getDocCount(), equalTo(2L));
            }, ValueType.NUMERIC
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD)
                .maxDocCount(2) // bump to 2 since we're only including "2"
                .includeExclude(new IncludeExclude(new String[]{"2"}, new String[]{})),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKeyAsString(), equalTo("2"));
                assertThat(bucket.getDocCount(), equalTo(2L));
            }, ValueType.STRING
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(DOUBLE_FIELD)
                .maxDocCount(2) // bump to 2 since we're only including "2"
                .includeExclude(new IncludeExclude(new double[]{2.0}, new double[]{})),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                DoubleTerms.Bucket bucket = (DoubleTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(2.0));
                assertThat(bucket.getDocCount(), equalTo(2L));
            }, ValueType.DOUBLE
        );
    }

    public void testEmbeddedMaxAgg() throws IOException {
        Query query = new MatchAllDocsQuery();

        testBothCases(query, dataset, aggregation -> {
                MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field(LONG_FIELD);
                aggregation.field(LONG_FIELD).maxDocCount(1).subAggregation(max);
            },
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(1L));
                assertThat(bucket.getDocCount(), equalTo(1L));

                Aggregations children = bucket.getAggregations();
                assertThat(children.asList().size(), equalTo(1));
                assertThat(children.asList().get(0).getName(), equalTo("the_max"));
                assertThat(((Max)(children.asList().get(0))).getValue(), equalTo(1L));
            }, ValueType.NUMERIC
        );
        /*
        testBothCases(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD).maxDocCount(1),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKeyAsString(), equalTo("1"));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }, ValueType.STRING
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(DOUBLE_FIELD).maxDocCount(1),
            agg -> {
                assertEquals(1, agg.getBuckets().size());
                DoubleTerms.Bucket bucket = (DoubleTerms.Bucket) agg.getBuckets().get(0);
                assertThat(bucket.getKey(), equalTo(1.0));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }, ValueType.DOUBLE
        );
        */
    }


    private <T> void termsAggregatorWithNestedMaxAgg(ValueType valueType, MappedFieldType fieldType,
                                                     Function<Integer, T> valueFactory,
                                                     Function<T, IndexableField> luceneFieldFactory) throws Exception {
        final Map<T, Long> counts = new HashMap<>();
        int numTerms = scaledRandomIntBetween(8, 128);
        for (int i = 0; i < numTerms; i++) {
            counts.put(valueFactory.apply(i), randomLong());
        }

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (Map.Entry<T, Long> entry : counts.entrySet()) {
                    Document document = new Document();
                    document.add(luceneFieldFactory.apply(entry.getKey()));
                    document.add(new NumericDocValuesField("value", entry.getValue()));
                    indexWriter.addDocument(document);
                }
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {

                    List<Map.Entry<T, Long>> expectedBuckets = new ArrayList<>(counts.entrySet());
                    Comparator<Map.Entry<T, Long>> comparator = Comparator.comparing(Map.Entry::getValue, Long::compareTo);
                    expectedBuckets.sort(comparator);

                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    AggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name", valueType)
                        .field("field")
                        .subAggregation(AggregationBuilders.max("_max").field("value"));
                    fieldType.setName("field");
                    fieldType.setHasDocValues(true);

                    MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                    fieldType2.setName("value");
                    fieldType2.setHasDocValues(true);
                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = (Terms) aggregator.buildAggregation(0L);
                    assertEquals(expectedBuckets.size(), result.getBuckets().size());
                    for (int i = 0; i < expectedBuckets.size(); i++) {
                        Map.Entry<T, Long>  expected = expectedBuckets.get(i);
                        Terms.Bucket actual = result.getBuckets().get(i);
                        assertEquals(expected.getKey(), actual.getKey());
                    }
                }
            }
        }
    }

    public void testEmpty() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType();
                fieldType1.setName("string");
                fieldType1.setHasDocValues(true);

                MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType1.setName("long");
                fieldType1.setHasDocValues(true);

                MappedFieldType fieldType3 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
                fieldType1.setName("double");
                fieldType1.setHasDocValues(true);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name", ValueType.STRING)
                        .field("string");
                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = (Terms) aggregator.buildAggregation(0L);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());

                    aggregationBuilder = new RareTermsAggregationBuilder("_name", ValueType.LONG)
                        .field("long");
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = (Terms) aggregator.buildAggregation(0L);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());

                    aggregationBuilder = new RareTermsAggregationBuilder("_name", ValueType.DOUBLE)
                        .field("double");
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType3);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = (Terms) aggregator.buildAggregation(0L);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());
                }
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("string", new BytesRef("a")));
                document.add(new NumericDocValuesField("long", 0L));
                document.add(new NumericDocValuesField("double", Double.doubleToRawLongBits(0L)));
                indexWriter.addDocument(document);
                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType();
                fieldType1.setName("another_string");
                fieldType1.setHasDocValues(true);

                MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType2.setName("another_long");
                fieldType2.setHasDocValues(true);

                MappedFieldType fieldType3 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
                fieldType3.setName("another_double");
                fieldType3.setHasDocValues(true);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    ValueType[] valueTypes = new ValueType[]{ValueType.STRING, ValueType.LONG, ValueType.DOUBLE};
                    String[] fieldNames = new String[]{"string", "long", "double"};
                    for (int i = 0; i < fieldNames.length; i++) {
                        RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name", valueTypes[i])
                            .field(fieldNames[i]);
                        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1, fieldType2, fieldType3);
                        aggregator.preCollection();
                        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                        aggregator.postCollection();
                        Terms result = (Terms) aggregator.buildAggregation(0L);
                        assertEquals("_name", result.getName());
                        assertEquals(0, result.getBuckets().size());
                    }
                }
            }
        }
    }

    public void testNestedTermsAgg() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("a")));
                document.add(new SortedDocValuesField("field2", new BytesRef("b")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("c")));
                document.add(new SortedDocValuesField("field2", new BytesRef("d")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("e")));
                document.add(new SortedDocValuesField("field2", new BytesRef("f")));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
                    RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name1", ValueType.STRING)
                        .field("field1")
                        .subAggregation(new RareTermsAggregationBuilder("_name2", ValueType.STRING)
                            .field("field2")
                        );
                    MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType();
                    fieldType1.setName("field1");
                    fieldType1.setHasDocValues(true);
                    MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType();
                    fieldType2.setName("field2");
                    fieldType2.setHasDocValues(true);

                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = (Terms) aggregator.buildAggregation(0L);
                    assertEquals(3, result.getBuckets().size());
                    assertEquals("a", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    Terms.Bucket nestedBucket = ((Terms) result.getBuckets().get(0).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("b", nestedBucket.getKeyAsString());
                    assertEquals("c", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    nestedBucket = ((Terms) result.getBuckets().get(1).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("d", nestedBucket.getKeyAsString());
                    assertEquals("e", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    nestedBucket = ((Terms) result.getBuckets().get(2).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("f", nestedBucket.getKeyAsString());
                }
            }
        }
    }

    public void testMixLongAndDouble() throws Exception {
        RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name", ValueType.LONG)
                .field("number");
            List<InternalAggregation> aggs = new ArrayList<> ();
            int numLongs = randomIntBetween(1, 3);
            for (int i = 0; i < numLongs; i++) {
                final Directory dir;
                try (IndexReader reader = createIndexWithLongs()) {
                    dir = ((DirectoryReader) reader).directory();
                    IndexSearcher searcher = new IndexSearcher(reader);
                    MappedFieldType fieldType =
                        new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                    fieldType.setName("number");
                    fieldType.setHasDocValues(true);
                    aggs.add(buildInternalAggregation(aggregationBuilder, fieldType, searcher));
                }
                dir.close();
            }
            int numDoubles = randomIntBetween(1, 3);
            for (int i = 0; i < numDoubles; i++) {
                final Directory dir;
                try (IndexReader reader = createIndexWithDoubles()) {
                    dir = ((DirectoryReader) reader).directory();
                    IndexSearcher searcher = new IndexSearcher(reader);
                    MappedFieldType fieldType =
                        new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
                    fieldType.setName("number");
                    fieldType.setHasDocValues(true);
                    aggs.add(buildInternalAggregation(aggregationBuilder, fieldType, searcher));
                }
                dir.close();
            }
            InternalAggregation.ReduceContext ctx =
                new InternalAggregation.ReduceContext(new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY),
                    new NoneCircuitBreakerService()), null, true);
            for (InternalAggregation internalAgg : aggs) {
                InternalAggregation mergedAggs = internalAgg.doReduce(aggs, ctx);
                assertTrue(mergedAggs instanceof DoubleTerms);
                long expected = numLongs + numDoubles;
                List<? extends Terms.Bucket> buckets = ((DoubleTerms) mergedAggs).getBuckets();
                assertEquals(4, buckets.size());
                assertEquals("1.0", buckets.get(0).getKeyAsString());
                assertEquals(expected, buckets.get(0).getDocCount());
                assertEquals("10.0", buckets.get(1).getKeyAsString());
                assertEquals(expected * 2, buckets.get(1).getDocCount());
                assertEquals("100.0", buckets.get(2).getKeyAsString());
                assertEquals(expected * 2, buckets.get(2).getDocCount());
                assertEquals("1000.0", buckets.get(3).getKeyAsString());
                assertEquals(expected, buckets.get(3).getDocCount());
            }
    }

    public void testGlobalAggregationWithScore() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("a")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("c")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("e")));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
                    GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global")
                        .subAggregation(
                            new RareTermsAggregationBuilder("terms", ValueType.STRING)
                                .field("keyword")
                                .subAggregation(
                                    new RareTermsAggregationBuilder("sub_terms", ValueType.STRING)
                                        .field("keyword")
                                        .subAggregation(
                                            new TopHitsAggregationBuilder("top_hits")
                                                .storedField("_none_")
                                        )
                                )
                        );

                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
                    fieldType.setName("keyword");
                    fieldType.setHasDocValues(true);

                    InternalGlobal result = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), globalBuilder, fieldType);
                    InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                    assertThat(terms.getBuckets().size(), equalTo(3));
                    for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                        InternalMultiBucketAggregation<?, ?> subTerms = bucket.getAggregations().get("sub_terms");
                        assertThat(subTerms.getBuckets().size(), equalTo(1));
                        MultiBucketsAggregation.Bucket subBucket  = subTerms.getBuckets().get(0);
                        InternalTopHits topHits = subBucket.getAggregations().get("top_hits");
                        assertThat(topHits.getHits().getHits().length, equalTo(1));
                        for (SearchHit hit : topHits.getHits()) {
                            assertThat(hit.getScore(), greaterThan(0f));
                        }
                    }
                }
            }
        }
    }

    public void testWithNestedAggregations() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 10; i++) {
                    int[] nestedValues = new int[i];
                    for (int j = 0; j < i; j++) {
                        nestedValues[j] = j;
                    }
                    indexWriter.addDocuments(generateDocsWithNested(Integer.toString(i), i, nestedValues));
                }
                indexWriter.commit();
                for (boolean withScore : new boolean[]{true, false}) {
                    NestedAggregationBuilder nested = new NestedAggregationBuilder("nested", "nested_object")
                        .subAggregation(new RareTermsAggregationBuilder("terms", ValueType.LONG)
                            .field("nested_value")
                            .subAggregation(
                                new TopHitsAggregationBuilder("top_hits")
                                    .sort(withScore ? new ScoreSortBuilder() : new FieldSortBuilder("_doc"))
                                    .storedField("_none_")
                            )
                        );
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                    fieldType.setHasDocValues(true);
                    fieldType.setName("nested_value");
                    try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                        InternalNested result = search(newSearcher(indexReader, false, true),
                            // match root document only
                            new DocValuesFieldExistsQuery(PRIMARY_TERM_NAME), nested, fieldType);
                        InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                        assertThat(terms.getBuckets().size(), equalTo(9));
                        int ptr = 9;
                        for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                            InternalTopHits topHits = bucket.getAggregations().get("top_hits");
                            assertThat(topHits.getHits().totalHits, equalTo((long) ptr));
                            if (withScore) {
                                assertThat(topHits.getHits().getMaxScore(), equalTo(1f));
                            } else {
                                assertThat(topHits.getHits().getMaxScore(), equalTo(Float.NaN));
                            }
                            --ptr;
                        }
                    }
                }
            }
        }
    }

    private final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
    private List<Document> generateDocsWithNested(String id, int value, int[] nestedValues) {
        List<Document> documents = new ArrayList<>();

        for (int nestedValue : nestedValues) {
            Document document = new Document();
            document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(TypeFieldMapper.NAME, "__nested_object", TypeFieldMapper.Defaults.FIELD_TYPE));
            document.add(new SortedNumericDocValuesField("nested_value", nestedValue));
            documents.add(document);
        }

        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "docs", TypeFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedNumericDocValuesField("value", value));
        document.add(sequenceIDFields.primaryTerm);
        documents.add(document);

        return documents;
    }


    private IndexReader createIndexWithLongs() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", 10));
        document.add(new SortedNumericDocValuesField("number", 100));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 1));
        document.add(new SortedNumericDocValuesField("number", 100));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 10));
        document.add(new SortedNumericDocValuesField("number", 1000));
        indexWriter.addDocument(document);
        indexWriter.close();
        return DirectoryReader.open(directory);
    }

    private IndexReader createIndexWithDoubles() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(10.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(100.0d)));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(1.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(100.0d)));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(10.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(1000.0d)));
        indexWriter.addDocument(document);
        indexWriter.close();
        return DirectoryReader.open(directory);
    }

    private InternalAggregation buildInternalAggregation(RareTermsAggregationBuilder builder, MappedFieldType fieldType,
                                                         IndexSearcher searcher) throws IOException {
        TermsAggregator aggregator = createAggregator(builder, searcher, fieldType);
        aggregator.preCollection();
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        return aggregator.buildAggregation(0L);
    }

    private void testSearchCase(Query query, List<Long> dataset,
                                Consumer<RareTermsAggregationBuilder> configure,
                                Consumer<InternalMappedRareTerms> verify, ValueType valueType) throws IOException {
        executeTestCase(false, query, dataset, configure, verify, valueType);
    }

    private void testSearchAndReduceCase(Query query, List<Long> dataset,
                                         Consumer<RareTermsAggregationBuilder> configure,
                                         Consumer<InternalMappedRareTerms> verify, ValueType valueType) throws IOException {
        executeTestCase(true, query, dataset, configure, verify, valueType);
    }

    private void testBothCases(Query query, List<Long> dataset,
                               Consumer<RareTermsAggregationBuilder> configure,
                               Consumer<InternalMappedRareTerms> verify, ValueType valueType) throws IOException {
        testSearchCase(query, dataset, configure, verify, valueType);
        testSearchAndReduceCase(query, dataset, configure, verify, valueType);
    }

    @Override
    protected IndexSettings createIndexSettings() {
        Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", 100000).build();
        return new IndexSettings(
            IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(boolean reduced, Query query, List<Long> dataset,
                                 Consumer<RareTermsAggregationBuilder> configure,
                                 Consumer<InternalMappedRareTerms> verify, ValueType valueType) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Long value : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    document.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(Long.toString(value))));
                    document.add(new SortedNumericDocValuesField(DOUBLE_FIELD, Double.doubleToRawLongBits((double) value)));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                RareTermsAggregationBuilder aggregationBuilder = new RareTermsAggregationBuilder("_name", valueType);
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType();
                keywordFieldType.setName(KEYWORD_FIELD);
                keywordFieldType.setHasDocValues(true);

                MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                longFieldType.setName(LONG_FIELD);
                longFieldType.setHasDocValues(true);

                MappedFieldType doubleFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
                doubleFieldType.setName(DOUBLE_FIELD);
                doubleFieldType.setHasDocValues(true);

                InternalMappedRareTerms rareTerms;
                if (reduced) {
                    rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, keywordFieldType, longFieldType, doubleFieldType);
                } else {
                    rareTerms = search(indexSearcher, query, aggregationBuilder, keywordFieldType, longFieldType, doubleFieldType);
                }
                verify.accept(rareTerms);
            }
        }
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         *
         * This is used in the aggregator tests to check that after a reduction, we have the correct number of buckets.
         * This can be done during incremental reduces, and the final reduce.  Unfortunately, the number of buckets
         * can _decrease_ during runtime as values are reduced together (e.g. 1 count on each shard, but when
         * reduced it becomes 2 and is greater than the threshold).
         *
         * Because the incremental reduction test picks random subsets to reduce together, it's impossible
         * to predict how the buckets will end up, and so this assertion will fail.
         *
         * If we want to put this assertion back in, we'll need this test to override the incremental reduce
         * portion so that we can deterministically know which shards are being reduced together and which
         * buckets we should have left after each reduction.
         */
    }


}
