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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Takes in SegmentedScanResults and launches several DynamoDBConsumerWorker for
 * each batch of items to write to a DynamoDB table.
 */
public class DynamoDBConsumer extends AbstractLogConsumer {

    private final AmazonDynamoDBClient client;
    private final String tableName;
    private final RateLimiter rateLimiter;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumer.class);

    /**
     * Class to consume logs and write them to a DynamoDB table.
     */
    public DynamoDBConsumer(AmazonDynamoDBClient client, String tableName,
            double rateLimit, ExecutorService exec) {
        this.client = client;
        this.tableName = tableName;
        this.rateLimiter = RateLimiter.create(rateLimit);
        super.threadPool = exec;
        super.exec = new ExecutorCompletionService<Void>(threadPool);
    }

    /**
     * calls splitResultIntoBatches to turn the SegmentedScanResult into several
     * BatchWriteItemRequests and then submits them as individual jobs to the
     * ExecutorService.
     */
    @Override
    public Future<Void> writeResult(SegmentedScanResult result) {
        Future<Void> jobSubmission = null;
        List<BatchWriteItemRequest> batches = splitResultIntoBatches(
                result.getScanResult().getItems(), tableName);
        Iterator<BatchWriteItemRequest> batchesIterator = batches.iterator();
        int batchesSubmitted = 0;
        int totalItemsSubmittedInCurrentBatch = 0;
        while (batchesIterator.hasNext()) {
            try {
                final BatchWriteItemRequest itemRequest = batchesIterator.next();
                totalItemsSubmittedInCurrentBatch += itemRequest.getRequestItems().values().stream().mapToInt(List::size).sum();
                jobSubmission = exec
                        .submit(new DynamoDBConsumerWorker(itemRequest, client, rateLimiter, tableName, totalItemsWritten, failedItems));
                batchesSubmitted++;
                LOGGER.info(String.format("Failed items: %s", failedItems.values().stream().mapToLong(Collection::size).sum()));
            } catch (NullPointerException npe) {
                throw new NullPointerException(
                        "Thread pool not initialized for LogStashExecutor");
            }
        }
        LOGGER.debug(String.format("%s batches submitted", batchesSubmitted));
        totalBatchesSubmitted += batchesSubmitted;
        LOGGER.debug(String.format("%s items submitted", totalItemsSubmittedInCurrentBatch));
        totalItemsSubmitted += totalItemsSubmittedInCurrentBatch;
        return jobSubmission;
    }

    @Override
    protected void putFailedItems(List<WriteRequest> failedRequests) {
        final List<Map<String, AttributeValue>> items = failedRequests.stream().map(WriteRequest::getPutRequest).map(PutRequest::getItem).collect(Collectors.toList());

        List<BatchWriteItemRequest> batches = splitResultIntoBatches(items, tableName);
        final AtomicInteger totalItemsWritten = new AtomicInteger(0);
        final ConcurrentHashMap<String, List<WriteRequest>> failedItems = new ConcurrentHashMap<>();
        for (BatchWriteItemRequest batch : batches) {
            new DynamoDBConsumerWorker(batch, client, rateLimiter, tableName, totalItemsWritten, failedItems).call();
        }

        LOGGER.info(String.format("%s total items written", totalItemsWritten.get()));
        final List<WriteRequest> newFailedRequests = failedItems.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        final String failedItemsStr = newFailedRequests.stream().map(this::formatRequests).collect(Collectors.joining(", "));
        LOGGER.info(String.format("Failed items: %s", failedItemsStr));
    }

    /**
     * Splits up a ScanResult into a list of BatchWriteItemRequests of size 25
     * items or less each.
     */
    public static List<BatchWriteItemRequest> splitResultIntoBatches(
            List<Map<String, AttributeValue>> items, String tableName) {
        List<BatchWriteItemRequest> batches = new LinkedList<BatchWriteItemRequest>();
        Iterator<Map<String, AttributeValue>> it = items.iterator();

        BatchWriteItemRequest req = new BatchWriteItemRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        List<WriteRequest> writeRequests = new LinkedList<WriteRequest>();
        int i = 0;
        while (it.hasNext()) {
            PutRequest put = new PutRequest(it.next());
            writeRequests.add(new WriteRequest(put));

            i++;
            if (i == BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM) {
                req.addRequestItemsEntry(tableName, writeRequests);
                batches.add(req);
                req = new BatchWriteItemRequest()
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                writeRequests = new LinkedList<WriteRequest>();
                i = 0;
            }
        }
        if (i > 0) {
            req.addRequestItemsEntry(tableName, writeRequests);
            batches.add(req);
        }
        LOGGER.debug(String.format("%s batches created.", batches.size()));
        return batches;
    }
}