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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Batch;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.producer.spinner.BatchWriterContext.BatchWriterState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The event action context for the batch writing protocol FSM
 * 
 * @author hhildebrand
 * 
 */
public class BatchWriter {

    private static final Logger        log      = LoggerFactory.getLogger(BatchWriter.class.getCanonicalName());
    private final Thread               consumer;
    private ByteBuffer[]               currentWrite;
    private final BatchWriterContext   fsm      = new BatchWriterContext(this);
    private SocketChannelHandler       handler;
    private boolean                    inError  = false;
    private final List<Batch>          inFlight = new ArrayList<Batch>();
    private final int                  maximumBatchedSize;
    private final Semaphore            quantum  = new Semaphore(0);
    private final BlockingDeque<Batch> queued;
    private final AtomicBoolean        run      = new AtomicBoolean(true);

    public BatchWriter(int maxQueueLength, int maximumBatchedSize,
                       String fsmName) {
        queued = new LinkedBlockingDeque<Batch>(maxQueueLength);
        this.maximumBatchedSize = maximumBatchedSize;
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
        if (!run.get()) {
            return;
        }
        pending.put(events, events);
        if (!queued.offer(events)) {
            pending.remove(events);
            throw new RateLimiteExceededException();
        }
    }

    protected void close() {
        queued.clear();
        run.set(false);
        quantum.release(10);
        if (handler != null) {
            handler.close();
        }
        currentWrite = null;
        inFlight.clear();
    }

    protected Runnable consumerAction() {
        return new Runnable() {

            @Override
            public void run() {
                while (run.get()) {
                    Batch first = null;
                    try {
                        quantum.acquire();
                        do {
                            first = queued.poll(4, TimeUnit.SECONDS);
                            if (!run.get()) {
                                return;
                            }
                        } while (first == null);
                    } catch (InterruptedException e) {
                        return;
                    }
                    inFlight.add(first);
                    queued.drainTo(inFlight, maximumBatchedSize - 1);
                    if (log.isTraceEnabled()) {
                        log.trace(String.format("pushing %s batches",
                                                inFlight.size()));
                    }
                    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
                    for (Batch batch : inFlight) {
                        batch.appendBuffersTo(buffers);
                    }
                    currentWrite = buffers.toArray(new ByteBuffer[buffers.size()]);
                    fsm.writeBatch();
                }
            }
        };
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
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
        try {
            long written = handler.getChannel().write(currentWrite);
            if (log.isTraceEnabled()) {
                log.trace(String.format("%s bytes written", written));
            }
            if (written < 0) {
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
        if (currentWrite[currentWrite.length - 1].hasRemaining()) {
            return false;
        }
        if (log.isInfoEnabled()) {
            StringBuilder builder = new StringBuilder(4096);
            builder.append('\n');
            int i = 0;
            for (Batch batch : inFlight) {
                builder.append('\t');
                builder.append(String.format("Pushed %s,%s,%s,%s",
                                             batch.header.getSequenceNumber(),
                                             batch.header.getChannel(),
                                             fsm.getName(),
                                             batch.header.getProducerMirror()));
                if (++i != inFlight.size()) {
                    builder.append('\n');
                }
            }
            log.info(builder.toString());
        }
        inFlight.clear();
        currentWrite = null;
        return true;
    }

    protected void writeReady() {
        fsm.writeReady();
    }
}
