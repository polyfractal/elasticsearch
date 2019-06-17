package org.elasticsearch.xpack.rollup.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;

import java.util.Collections;
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
public class SingleQueryRollupIndexer extends RollupIndexer {
    private static final Logger logger = LogManager.getLogger(SingleQueryRollupIndexer.class.getName());

    private final Client client;
    private final RollupJob job;
    private final RollupJobTask task;
    private final CompositeAggregationBuilder compositeBuilder;

    SingleQueryRollupIndexer(RollupJob job, IndexerState initialState, Map<String, Object> initialPosition,
                             Client client, ThreadPool threadPool, RollupJobTask task) {
        super(threadPool.executor(ThreadPool.Names.GENERIC), job, new AtomicReference<>(initialState), initialPosition);
        this.client = client;
        this.job = job;
        this.task = task;
        this.compositeBuilder = createCompositeBuilder(job.getConfig());
    }

    @Override
    protected List<SearchRequest> buildSearchRequests() {
        final Map<String, Object> position = getPosition();
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
            .size(0)
            .trackTotalHits(false)
            // make sure we always compute complete buckets that appears before the configured delay
            .query(createBoundaryQuery(position))
            .aggregation(compositeBuilder.aggregateAfter(position));
        return Collections.singletonList(new SearchRequest(job.getConfig().getIndexPattern()).source(searchSource));
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
        assert searchResponses.size() == 1;
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
}
