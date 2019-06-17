package org.elasticsearch.xpack.rollup.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link RollupIndexer} that uses a {@link Client} to perform search
 * and bulk requests.
 * It uses the {@link ThreadPool.Names#GENERIC} thread pool to fire the first request in order
 * to make sure that we never use the same thread than the persistent task to execute the rollup.
 * The execution in the generic thread pool should terminate quickly since we use async call in the {@link Client}
 * to perform all requests.
 */
public class MultiQueryRollupIndexer extends RollupIndexer {
    private static final Logger logger = LogManager.getLogger(MultiQueryRollupIndexer.class.getName());

    private final Client client;
    private final RollupJob job;
    private final RollupJobTask task;
    private final Map<String, CompositeAggregationBuilder> compositeBuilders;
    private long minTimestamp;

    MultiQueryRollupIndexer(RollupJob job, IndexerState initialState, Map<String, Object> initialPosition,
                             Client client, ThreadPool threadPool, RollupJobTask task) {
        super(threadPool.executor(ThreadPool.Names.GENERIC), job, new AtomicReference<>(initialState), initialPosition);
        this.client = client;
        this.job = job;
        this.task = task;
        int numQueries = job.getConfig().getConcurrentQueries();
        compositeBuilders = new HashMap<>(numQueries);
    }

    @Override
    protected List<SearchRequest> buildSearchRequests() {

        final Map<String, Object> position = getPosition();


        if (position == null) {
            DateHistogramGroupConfig dateHistoGroup = job.getConfig().getGroupConfig().getDateHistogram();
            if (dateHistoGroup.getIntervalTypeName().equals(DateHistogramGroupConfig.FixedInterval.TYPE_NAME)) {
                return getFirstRunFixedQueries();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private List<SearchRequest> getFirstRunFixedQueries() {
        int numQueries = job.getConfig().getConcurrentQueries();
        List<SearchRequest> requests = new ArrayList<>(numQueries);
        DateHistogramGroupConfig dateHistoGroup = job.getConfig().getGroupConfig().getDateHistogram();

        // We know this is a fixed interval, so this "estimation" is correct, e.g. parses millis exactly internally
        long interval = dateHistoGroup.getInterval().estimateMillis();

        for (int i = 0; i < numQueries; i++) {

            long start = dateHistoGroup.createRounding().round(minTimestamp + (interval * i));
            long end = dateHistoGroup.createRounding().round(minTimestamp + ((interval + 1) * i));
            RangeQueryBuilder range = new RangeQueryBuilder(job.getConfig().getGroupConfig().getDateHistogram().getField())
                .gte(start)
                .lt(end <= maxBoundary ? end : maxBoundary) // TODO we can make this smarter e.g. for empty ranges
                .format("epoch_millis");

            SearchSourceBuilder builder = new SearchSourceBuilder()
                .size(0)
                .trackTotalHits(false)
                // make sure we always compute complete buckets that appears before the configured delay
                .query(new BoolQueryBuilder().filter(range))
                .aggregation(createCompositeBuilder(job.getConfig()));
            requests.add(new SearchRequest(job.getConfig().getIndexPattern()).source(builder));
        }
        return requests;
    }

    /**
     * Creates the range query that limits the search to documents that appear before the maximum allowed time
     * (see {@link #maxBoundary}
     * and on or after the last processed time.
     * @param position The current position of the pagination
     * @return The range query to execute
     */
    private QueryBuilder createBoundaryQuery(Map<String, Object> position) {
        assert maxBoundary < Long.MAX_VALUE;
        DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
        String fieldName = dateHisto.getField();
        String rollupFieldName = fieldName + "."  + DateHistogramAggregationBuilder.NAME;
        long lowerBound = 0L;
        if (position != null) {
            Number value = (Number) position.get(rollupFieldName);
            lowerBound = value.longValue();
        }
        assert lowerBound <= maxBoundary;
        final RangeQueryBuilder query = new RangeQueryBuilder(fieldName)
            .gte(lowerBound)
            .lt(maxBoundary)
            .format("epoch_millis");
        return query;
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(List<SearchResponse> searchResponses) {
        assert searchResponses.size() >= 1;
        final CompositeAggregation response = searchResponses.get(0).getAggregations().get(AGGREGATION_NAME);

        if (response.getBuckets().isEmpty()) {
            // do not reset the position as we want to continue from where we stopped
            return new IterationResult<>(Collections.emptyList(), getPosition(), true);
        }

        return new IterationResult<>(
            IndexerUtils.processBuckets(response, job.getConfig().getRollupIndex(), getStats(),
                job.getConfig().getGroupConfig(), job.getConfig().getId()),
            response.afterKey(), response.getBuckets().isEmpty());
    }

    @Override
    protected void doNextSearch(List<SearchRequest> requests, ActionListener<SearchResponse> nextPhase) {
        requests.forEach(request -> ClientHelper.executeWithHeadersAsync(job.getHeaders(),
            ClientHelper.ROLLUP_ORIGIN, client, SearchAction.INSTANCE, request, nextPhase)
        );
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.ROLLUP_ORIGIN, client, BulkAction.INSTANCE, request,
            nextPhase);
    }

    @Override
    protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
        if (indexerState.equals(IndexerState.ABORTING)) {
            // If we're aborting, just invoke `next` (which is likely an onFailure handler)
            next.run();
        } else {
            // Otherwise, attempt to persist our state
            final RollupJobStatus state = new RollupJobStatus(indexerState, getPosition());
            //logger.debug("Updating persistent state of job [" + job.getConfig().getId() + "] to [" + indexerState.toString() + "]");
            task.updatePersistentTaskState(state, ActionListener.wrap(task -> next.run(), exc -> next.run()));
        }
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        logger.debug("Finished indexing for job [" + job.getConfig().getId() + "]");
        listener.onResponse(null);
    }

    @Override
    protected void onFailure(Exception exc) {
        logger.warn("Rollup job [" + job.getConfig().getId() + "] failed with an exception: ", exc);
    }

    @Override
    protected void onAbort() {
        task.shutdown();
    }

    @Override

    protected void onStart(long now, ActionListener<Void> listener) {
        // Since we have to filter time ranges explicitly, we need to find the min timestamp
        String fieldName = job.getConfig().getGroupConfig().getDateHistogram().getField();
        SearchSourceBuilder builder = new SearchSourceBuilder()
            .size(0)
            .trackTotalHits(false)
            .query(new MatchAllQueryBuilder()) // ensure we only look at docs with a timestamp
            .aggregation(new MinAggregationBuilder("min_timestamp").field(fieldName).format("epoch_millis"));

        SearchRequest request = new SearchRequest(job.getConfig().getIndexPattern()).source(builder);

        client.search(request, ActionListener.wrap(searchResponse -> {
            if (searchResponse.isTimedOut()) {
                listener.onFailure(new ElasticsearchTimeoutException("Rollup job [" + job.getConfig().getId() + "]" +
                    " timed out while performing initial minimum boundary check. Required to proceed, aborting."));
                return;
            } else if (searchResponse.getFailedShards() > 0) {
                listener.onFailure(new RuntimeException("Rollup job [" + job.getConfig().getId() + "]" +
                    " had shard failures  while performing initial minimum boundary check. Required to proceed, aborting.",
                    searchResponse.getShardFailures()[0].getCause()));
                return;
            }

            minTimestamp = Long.parseLong(((InternalMin) searchResponse.getAggregations().asMap().get("min_timestamp")).getValueAsString());

            // found min timestamp, continue
            listener.onResponse(null);
        }, listener::onFailure));
    }
}
