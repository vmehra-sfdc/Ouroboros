/**
 * Copyright (c) 2011, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.ouroboros.producer.spinner;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Batch;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.producer.spinner.BatchWriterContext.BatchWriterFSM;
import com.salesforce.ouroboros.producer.spinner.BatchWriterContext.BatchWriterState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The event action context for the batch writing protocol FSM
 * 
 * @author hhildebrand
 * 
 */
public class BatchWriter {

    private static final Logger        log          = LoggerFactory.getLogger(BatchWriter.class.getCanonicalName());
    private Batch                      batch;
    private final Thread               consumer;
    private Batch                      freeList;
    private final ReentrantLock        freeListLock = new ReentrantLock();
    private final BatchWriterContext   fsm          = new BatchWriterContext(
                                                                             this);
    private SocketChannelHandler       handler;
    private boolean                    inError      = false;
    private final Semaphore            quantum      = new Semaphore(0);
    private final BlockingQueue<Batch> queued;
    private final AtomicBoolean        run          = new AtomicBoolean(true);

    public BatchWriter(int maxQueueLength, String fsmName) {
        queued = new ArrayBlockingQueue<Batch>(maxQueueLength);
        fsm.setName(fsmName);
        consumer = new Thread(
                              consumerAction(),
                              String.format("Consumer thread for BatchWriter[%s]",
                                            fsmName));
        consumer.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn(String.format("Uncaught exception on %s", t), e);
            }
        });
        consumer.setDaemon(true);
        consumer.start();
    }

    public void closing() {
        fsm.close();
    }

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.connect();
    }

    public void failover() {
        fsm.failover();
    }

    public BatchWriterState getState() {
        return fsm.getState();
    }

    /**
     * Push the batch of events into the system. If the push is successful, the
     * batch of events will be added to the list of pending events, waiting for
     * acknowledgement.
     * 
     * @param events
     *            - the batch of events to push
     * @param pending
     *            - the map of pending events to record the batch, if the push
     *            can continue.
     * @throws RateLimiteExceededException
     *             - if the limit on the number of queued batches has been
     *             exceeded.
     */
    public void push(Batch events, Map<BatchIdentity, Batch> pending)
                                                                     throws RateLimiteExceededException {
        if (fsm.getState() == BatchWriterFSM.Closed) {
            return;
        }
        pending.put(events, events);
        if (!queued.offer(events)) {
            pending.remove(events);
            throw new RateLimiteExceededException();
        }
    }

    private void free() {
        final ReentrantLock lock = freeListLock;
        lock.lock();
        try {
            freeList = batch.link(freeList);
            batch = null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param mirrorProducer
     * @param channel
     * @param sequenceNumber
     * @param events
     * @return
     */
    protected Batch allocateBatch() {
        final ReentrantLock lock = freeListLock;
        lock.lock();
        try {
            if (freeList == null) {
                return new Batch();
            }
            Batch allocated = freeList;
            freeList = allocated.delink();
            return allocated;
        } finally {
            lock.unlock();
        }
    }

    protected void close() {
        queued.clear();
        run.set(false);
        quantum.release(10);
        if (handler != null) {
            handler.close();
        }
        batch = null;
        freeList = null;
    }

    protected Runnable consumerAction() {
        return new Runnable() {

            @Override
            public void run() {
                while (run.get()) {
                    try {
                        quantum.acquire();
                        while (batch == null) {
                            batch = queued.poll(4, TimeUnit.SECONDS);
                            if (!run.get()) {
                                return;
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    fsm.writeBatch();
                }
            }
        };
    }

    protected boolean hasNext() {
        batch = queued.poll();
        return batch != null;
    }

    protected boolean hasPendingBatch() {
        return queued.poll() != null;
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
        batch.rewind();
        if (writeBatch()) {
            fsm.payloadWritten();
        } else {
            if (inError) {
                fsm.close();
            } else {
                if (inError) {
                    fsm.close();
                } else {
                    handler.selectForWrite();
                }
            }
        }
    }

    protected void nextQuantum() {
        quantum.release();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        if (batch.header.hasRemaining()) {
            try {
                if (batch.header.write(handler.getChannel()) < 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("Closing channel");
                    }
                    inError = true;
                    return false;
                }
            } catch (IOException e) {
                if (Utils.isClose(e)) {
                    log.info(String.format("closing batch writer %s ",
                                           fsm.getName()));
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(String.format("Unable to write event batch payload %s",
                                               handler.getChannel()), e);
                    }
                }
                inError = true;
                return false;
            }
            if (batch.header.hasRemaining()) {
                return false;
            }
        }
        try {
            if (handler.getChannel().write(batch.batch) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing batch writer %s ",
                                       fsm.getName()));
            } else {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Unable to write event batch payload %s",
                                           handler.getChannel()), e);
                }
            }
            inError = true;
            return false;
        }
        if (batch.batch.hasRemaining()) {
            return false;
        }
        if (log.isInfoEnabled()) {
            log.info(String.format("Pushed %s,%s,%s,%s",
                                   batch.header.getSequenceNumber(),
                                   batch.header.getChannel(), fsm.getName(),
                                   batch.header.getProducerMirror()));
        }
        free();
        return true;
    }

    protected void writeReady() {
        fsm.writeReady();
    }
}
