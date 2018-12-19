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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;

/**
 * The interface to use with the DynamoDBBootstrapWorker.java class to consume
 * logs and write the results somewhere.
 */
public abstract class AbstractLogConsumer {

    public ExecutorCompletionService<Void> exec;
    protected ExecutorService threadPool;
    protected int totalBatchesSubmitted = 0;
    protected int totalItemsSubmitted = 0;
    protected final AtomicInteger totalItemsWritten = new AtomicInteger(0);
    protected final ConcurrentHashMap<String, List<WriteRequest>> failedItems = new ConcurrentHashMap<>();

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(AbstractLogConsumer.class);

    /**
     * Writes the result of a scan to another endpoint asynchronously. Will call
     * getWorker to determine what job to submit with the result.
     * 
     * @param <result>
     *            the SegmentedScanResult to asynchronously write to another
     *            endpoint.
     */
    public abstract Future<Void> writeResult(SegmentedScanResult result);

    /**
     * Shuts the thread pool down.
     * 
     * @param <awaitTermination>
     *            If true, this method waits for the threads in the pool to
     *            finish. If false, this thread pool shuts down without
     *            finishing their current tasks.
     */
    public void shutdown(boolean awaitTermination) {
        if (awaitTermination) {
            boolean interrupted = false;
            threadPool.shutdown();
            try {
                while (!threadPool.awaitTermination(BootstrapConstants.WAITING_PERIOD_FOR_THREAD_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Waiting for the threadpool to terminate...");
                }
            } catch (InterruptedException e) {
                interrupted = true;
                LOGGER.warn("Threadpool was interrupted when trying to shutdown: "
                        + e.getMessage());
            } finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        } else {
            threadPool.shutdownNow();
        }
        LOGGER.info(String.format("%s total batches written", totalBatchesSubmitted));
        LOGGER.info(String.format("%s total items submitted", totalItemsSubmitted));
        LOGGER.info(String.format("%s total items written", totalItemsWritten.get()));
        final List<WriteRequest> failedRequests = failedItems.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        final String failedItemsStr = failedRequests.stream().map(this::formatRequests).collect(Collectors.joining(", "));
        LOGGER.info(String.format("Failed items: %s", failedItemsStr));
        LOGGER.info("Sending failed items");
        putFailedItems(failedRequests);
    }

    protected String formatRequests(WriteRequest request) {
        return request.getPutRequest().getItem().get("cnfFingerprint").getS();
    }

    protected abstract void putFailedItems(List<WriteRequest> failedRequests);
}