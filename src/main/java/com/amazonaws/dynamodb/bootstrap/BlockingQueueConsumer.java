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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.dynamodb.bootstrap.DynamoDBEntryWithSize;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * This class implements ILogConsumer, and when called to writeResult, it will
 * submit a new job to it's ExecutorCompletionService with a new
 * LogStashQueueWorker. It will then shutdown by adding a 'poison pill' to the
 * end of the blocking queue to notify that it has reached the end of the scan.
 */
public class BlockingQueueConsumer extends AbstractLogConsumer {

    private BlockingQueue<DynamoDBEntryWithSize> queue;

    public BlockingQueueConsumer(int numThreads) {
        this.queue = new ArrayBlockingQueue<DynamoDBEntryWithSize>(20);
        int numProcessors = Runtime.getRuntime().availableProcessors();
        if (numProcessors > numThreads) {
            numThreads = numProcessors;
        }
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        this.exec = new ExecutorCompletionService<Void>(threadPool);
    }

    @Override
    public Future<Void> writeResult(SegmentedScanResult result) {
        Future<Void> jobSubmission = null;
        try {
            jobSubmission = exec.submit(new BlockingQueueWorker(queue, result));
        } catch (NullPointerException npe) {
            throw new NullPointerException(
                    "Thread pool not initialized for LogStashExecutor");
        }
        return jobSubmission;
    }

    /**
     * Returns the blocking queue to which the LogStashQueueWorkers add results
     */
    public BlockingQueue<DynamoDBEntryWithSize> getQueue() {
        return queue;
    }

    /**
     * Shuts down the threadpool then adds a termination result to the end of
     * the queue.
     */
    @Override
    public void shutdown(boolean awaitTermination) {
        super.shutdown(awaitTermination);
        boolean added = false;
        boolean interrupted = false;
        try {
            do {
                try {
                    // a null entry with -1 size to notify the receiver that the
                    // queue is done processing entries
                    queue.put(new DynamoDBEntryWithSize(null, -1));
                    added = true;
                } catch (InterruptedException e) {
                    interrupted = true;
                    continue;
                }
            } while (!added);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void putFailedItems(List<WriteRequest> failedRequests) {
        throw new NotImplementedException();
    }

}
