/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.collapser;

import java.lang.ref.Reference;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import com.netflix.hystrix.HystrixCollapserProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixConcurrencyStrategy;
import com.netflix.hystrix.strategy.concurrency.HystrixContextCallable;
import com.netflix.hystrix.util.HystrixTimer.TimerListener;

/**
 * Requests are submitted to this and batches executed based on size or time. Scoped to either a request or the global application.
 * <p>
 * Instances of this are retrieved from the RequestCollapserFactory.
 * 
 * Must be thread-safe since it exists within a RequestVariable which is request-scoped and can be accessed from multiple threads.
 * 
 * @ThreadSafe
 *   1）提交单个命令请求到请求队列( RequestQueue )
 *   2）接收来自定时任务提交的多个命令，合并执行
 */
public class RequestCollapser<BatchReturnType, ResponseType, RequestArgumentType> {
    static final Logger logger = LoggerFactory.getLogger(RequestCollapser.class);
    static final Object NULL_SENTINEL = new Object();

    private final HystrixCollapserBridge<BatchReturnType, ResponseType, RequestArgumentType> commandCollapser;
    // batch can be null once shutdown
    private final AtomicReference<RequestBatch<BatchReturnType, ResponseType, RequestArgumentType>> batch = new AtomicReference<RequestBatch<BatchReturnType, ResponseType, RequestArgumentType>>();
    private final AtomicReference<Reference<TimerListener>> timerListenerReference = new AtomicReference<Reference<TimerListener>>();
    private final AtomicBoolean timerListenerRegistered = new AtomicBoolean();
    private final CollapserTimer timer;
    private final HystrixCollapserProperties properties;
    private final HystrixConcurrencyStrategy concurrencyStrategy;

    /**
     * @param commandCollapser collapser which will create the batched requests and demultiplex the results
     * @param properties collapser properties that define how collapsing occurs
     * @param timer {@link CollapserTimer} which performs the collapsing
     * @param concurrencyStrategy strategy for managing the {@link Callable}s generated by {@link RequestCollapser}
     */
    RequestCollapser(HystrixCollapserBridge<BatchReturnType, ResponseType, RequestArgumentType> commandCollapser, HystrixCollapserProperties properties, CollapserTimer timer, HystrixConcurrencyStrategy concurrencyStrategy) {
        this.commandCollapser = commandCollapser; // the command with implementation of abstract methods we need 
        this.concurrencyStrategy = concurrencyStrategy;
        this.properties = properties;
        this.timer = timer;
        batch.set(new RequestBatch<BatchReturnType, ResponseType, RequestArgumentType>(properties, commandCollapser, properties.maxRequestsInBatch().get()));
    }

    /**
     * Submit a request to a batch. If the batch maxSize is hit trigger the batch immediately.
     * 
     * @param arg argument to a {@link RequestCollapser}
     * @return Observable<ResponseType>
     * @throws IllegalStateException
     *             if submitting after shutdown
     */
    public Observable<ResponseType> submitRequest(final RequestArgumentType arg) {
        /*
         * We only want the timer ticking if there are actually things to do so we register it the first time something is added.
         */
        if (!timerListenerRegistered.get() && timerListenerRegistered.compareAndSet(false, true)) {
            /* schedule the collapsing task to be executed every x milliseconds (x defined inside CollapsedTask) */
            timerListenerReference.set(timer.addListener(new CollapsedTask()));
        }

        // loop until succeed (compare-and-set spin-loop)
        while (true) {
            final RequestBatch<BatchReturnType, ResponseType, RequestArgumentType> b = batch.get();
            if (b == null) {
                return Observable.error(new IllegalStateException("Submitting requests after collapser is shutdown"));
            }

            final Observable<ResponseType> response;
            if (arg != null) {
                response = b.offer(arg);
            } else {
                //特殊处理，因为ConcurrentHashMap ，不允许值为 null
                response = b.offer( (RequestArgumentType) NULL_SENTINEL);
            }
            // it will always get an Observable unless we hit the max batch size
            if (response != null) {
                return response;
            } else {
                // this batch can't accept requests so create a new one and set it if another thread doesn't beat us
                createNewBatchAndExecutePreviousIfNeeded(b);
            }
        }
    }

    private void createNewBatchAndExecutePreviousIfNeeded(RequestBatch<BatchReturnType, ResponseType, RequestArgumentType> previousBatch) {
        if (previousBatch == null) {
            throw new IllegalStateException("Trying to start null batch which means it was shutdown already.");
        }
        //通过 CAS 修改 batch ，保证并发情况下的线程安全。同时注意，此处也进行了新的 RequestBatch ，切换掉老的 RequestBatch
        if (batch.compareAndSet(previousBatch, new RequestBatch<BatchReturnType, ResponseType, RequestArgumentType>(properties, commandCollapser, properties.maxRequestsInBatch().get()))) {
            // this thread won so trigger the previous batch
            //使用老的 RequestBatch ，调用 RequestBatch#executeBatchIfNotAlreadyStarted() 方法，命令合并执行。
            previousBatch.executeBatchIfNotAlreadyStarted();
        }
    }

    /**
     * Called from RequestVariable.shutdown() to unschedule the task.
     */
    public void shutdown() {
        RequestBatch<BatchReturnType, ResponseType, RequestArgumentType> currentBatch = batch.getAndSet(null);
        if (currentBatch != null) {
            currentBatch.shutdown();
        }

        if (timerListenerReference.get() != null) {
            // if the timer was started we'll clear it so it stops ticking
            timerListenerReference.get().clear();
        }
    }

    /**
     * Executed on each Timer interval execute the current batch if it has requests in it.
     */
    private class CollapsedTask implements TimerListener {
        final Callable<Void> callableWithContextOfParent;

        CollapsedTask() {
            // this gets executed from the context of a HystrixCommand parent thread (such as a Tomcat thread)
            // so we create the callable now where we can capture the thread context
            callableWithContextOfParent = new HystrixContextCallable<Void>(concurrencyStrategy, new Callable<Void>() {
                // the wrapCallable call allows a strategy to capture thread-context if desired

                @Override
                public Void call() throws Exception {
                    try {
                        // we fetch current so that when multiple threads race
                        // we can do compareAndSet with the expected/new to ensure only one happens
                        RequestBatch<BatchReturnType, ResponseType, RequestArgumentType> currentBatch = batch.get();
                        // 1) it can be null if it got shutdown
                        // 2) we don't execute this batch if it has no requests and let it wait until next tick to be executed
                        if (currentBatch != null && currentBatch.getSize() > 0) {
                            // do execution within context of wrapped Callable
                            createNewBatchAndExecutePreviousIfNeeded(currentBatch);
                        }
                    } catch (Throwable t) {
                        logger.error("Error occurred trying to execute the batch.", t);
                        t.printStackTrace();
                        // ignore error so we don't kill the Timer mainLoop and prevent further items from being scheduled
                    }
                    return null;
                }

            });
        }

        @Override
        public void tick() {
            try {
                callableWithContextOfParent.call();
            } catch (Exception e) {
                logger.error("Error occurred trying to execute callable inside CollapsedTask from Timer.", e);
                e.printStackTrace();
            }
        }

        @Override
        public int getIntervalTimeInMilliseconds() {
            return properties.timerDelayInMilliseconds().get();
        }

    }

}
