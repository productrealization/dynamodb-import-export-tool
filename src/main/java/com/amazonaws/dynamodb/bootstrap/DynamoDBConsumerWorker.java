/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Callable class that is used to write a batch of items to DynamoDB with exponential backoff.
 */
public class DynamoDBConsumerWorker implements Callable<Void> {

    private final AmazonDynamoDBClient client;
    private final RateLimiter rateLimiter;
    private long exponentialBackoffTime;
    private BatchWriteItemRequest batch;
    private final String tableName;
    private final AtomicInteger totalItemsWritten;
    private final ConcurrentHashMap<String, List<WriteRequest>> failedItems;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker.class);

    /**
     * Callable class that when called will try to write a batch to a DynamoDB
     * table. If the write returns unprocessed items it will exponentially back
     * off until it succeeds.
     */
    public DynamoDBConsumerWorker(BatchWriteItemRequest batchWriteItemRequest,
                                  AmazonDynamoDBClient client, RateLimiter rateLimiter,
                                  String tableName,
                                  final AtomicInteger totalItemsWritten,
                                  final ConcurrentHashMap<String, List<WriteRequest>> failedItems) {
        this.batch = batchWriteItemRequest;
        this.client = client;
        this.rateLimiter = rateLimiter;
        this.tableName = tableName;
        this.exponentialBackoffTime = BootstrapConstants.INITIAL_RETRY_TIME_MILLISECONDS;
        this.totalItemsWritten = totalItemsWritten;
        this.failedItems = failedItems;
    }

    /**
     * Batch writes the write request to the DynamoDB endpoint and THEN acquires
     * permits equal to the consumed capacity of the write.
     */
    @Override
    public Void call() {
        List<ConsumedCapacity> batchResult = runWithBackoff(batch);
        Iterator<ConsumedCapacity> it = batchResult.iterator();
        int consumedCapacity = 0;
        while (it.hasNext()) {
            consumedCapacity += it.next().getCapacityUnits().intValue();
        }
        rateLimiter.acquire(consumedCapacity);
        return null;
    }

    /**
     * Writes to DynamoDBTable using an exponential backoff. If the
     * batchWriteItem returns unprocessed items then it will exponentially
     * backoff and retry the unprocessed items.
     */
    public List<ConsumedCapacity> runWithBackoff(BatchWriteItemRequest req) {
        BatchWriteItemResult writeItemResult = null;
        final String requestId = UUID.randomUUID().toString();
        List<ConsumedCapacity> consumedCapacities = new LinkedList<ConsumedCapacity>();
        boolean interrupted = false;
        int retries = 0;
        try {
            do {
                writeItemResult = client.batchWriteItem(req);
                final List<WriteRequest> writeRequests = req.getRequestItems().get(tableName);
                consumedCapacities
                        .addAll(writeItemResult.getConsumedCapacity());

                if (notAllProcessed(writeItemResult)) {
                    final Map<String, List<WriteRequest>> unprocessedItems = writeItemResult.getUnprocessedItems();
                    final List<WriteRequest> unprocessedRequests = unprocessedItems.get(tableName);

                    totalItemsWritten.addAndGet(writeRequests.size() - unprocessedRequests.size());

                    retries++;

                    LOGGER.info(String.format("Request ID: %s. %s unprocessed items from batch of size %s, retry %s.",
                            requestId, unprocessedRequests.size(), writeRequests.size(), retries));
                    failedItems.put(requestId, unprocessedRequests);
//                    LOGGER.info(String.format("Request ID: %s. Items cnfFingerprints: [%s]", requestId, formatRequests(unprocessedRequests)));

                    req.setRequestItems(unprocessedItems);

                    try {
                        Thread.sleep(exponentialBackoffTime);
                    } catch (InterruptedException ie) {
                        LOGGER.error(String.format("Request ID: %s. Interrupted when waiting to write failed batch items", requestId));
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime *= 2;
                        if (exponentialBackoffTime > BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME) {
                            exponentialBackoffTime = BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME;
                        }
                    }
                } else {
                    totalItemsWritten.addAndGet(writeRequests.size());
                    if (retries > 0) {
                        LOGGER.info(String.format("Request ID: %s. Successful after %s retries.", requestId, retries));
                        failedItems.remove(requestId);
                    }
                }
            } while (notAllProcessed(writeItemResult));
            return consumedCapacities;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean notAllProcessed(BatchWriteItemResult result) {
        final Map<String, List<WriteRequest>> unprocessedItems = result.getUnprocessedItems();
        return unprocessedItems != null && unprocessedItems.get(tableName) != null;
    }
}
