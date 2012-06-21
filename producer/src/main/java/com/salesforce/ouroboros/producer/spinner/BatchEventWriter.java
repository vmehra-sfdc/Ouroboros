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
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
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
import com.salesforce.ouroboros.producer.spinner.BatchEventWriterContext.BatchEventWriterState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The event action context for the batch writing protocol FSM
 * 
 * @author hhildebrand
 * 
 */
public class BatchEventWriter {

    private static final Logger           log      = LoggerFactory.getLogger(BatchEventWriter.class.getCanonicalName());
    private final Thread                  consumer;
    private final ByteBuffer[]            currentWrite;
    private final BatchEventWriterContext fsm      = new BatchEventWriterContext(
                                                                                 this);
    private SocketChannelHandler          handler;
    private boolean                       inError  = false;
    private final Deque<Batch>            inFlight = new LinkedList<Batch>();
    private final int                     maximumBatchedSize;
    private final Semaphore               quantum  = new Semaphore(0);
    private final BlockingDeque<Batch>    queued;
    private final AtomicBoolean           run      = new AtomicBoolean(true);

    public BatchEventWriter(int maxQueueLength, int maximumBatchedSize,
                            String fsmName) {
        queued = new LinkedBlockingDeque<Batch>(maxQueueLength);
        this.maximumBatchedSize = maximumBatchedSize;
        currentWrite = new ByteBuffer[maximumBatchedSize * 2];
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

    public BatchEventWriterState getState() {
        return fsm.getState();
    }

    /**
     * @return
     */
    public boolean hasInflight() {
        return !inFlight.isEmpty();
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

    private void compactPushed() {
        StringBuilder builder = null;
        if (log.isInfoEnabled()) {
            builder = new StringBuilder(4096);
        }
        int currentSize = inFlight.size();
        for (int i = 0; i < currentSize; i += 2) {
            if (currentWrite[i].hasRemaining()
                || currentWrite[i + 1].hasRemaining()) {
                break;
            }
            Batch batch = inFlight.remove();
            if (log.isInfoEnabled()) {
                builder.append('\n');
                builder.append('\t');
                builder.append(String.format("Pushed %s,%s,%s,%s",
                                             batch.header.getSequenceNumber(),
                                             batch.header.getChannel(),
                                             fsm.getName(),
                                             batch.header.getProducerMirror()));
            }
        }
        int completed = currentSize - inFlight.size();
        if (completed != 0) {
            System.arraycopy(currentWrite, completed * 2, currentWrite, 0,
                             inFlight.size() * 2);
            Arrays.fill(currentWrite, inFlight.size() * 2, currentWrite.length,
                        null);
            if (log.isInfoEnabled()) {
                log.info(builder.toString());
            }
        }
    }

    @SuppressWarnings("unused")
    private boolean sanityCheck() {
        int i = 0;
        for (Batch batch : inFlight) {
            if (currentWrite[i++] == null) {
                System.out.println(String.format("Inflight does not match current write.  Index %s is null in current write",
                                                 i));
                return false;
            }
            if (currentWrite[i++] == null) {
                System.out.println(String.format("Inflight does not match current write.  Index %s is null in current write",
                                                 i));
                return false;
            }
        }
        for (; i < currentWrite.length; i++) {
            if (currentWrite[i] != null) {
                System.out.println(String.format(" Index %s is not null in current write",
                                                 i));
                return false;
            }
        }
        return true;
    }

    protected void close() {
        queued.clear();
        run.set(false);
        quantum.release(10);
        if (handler != null) {
            handler.close();
        }
        Arrays.fill(currentWrite, null);
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
                    fsm.writeBatch();
                }
            }
        };
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
        queued.drainTo(inFlight, maximumBatchedSize - inFlight.size());
        if (log.isTraceEnabled()) {
            log.trace(String.format("pushing %s batches", inFlight.size()));
        }
        int i = 0;
        for (Batch batch : inFlight) {
            currentWrite[i++] = batch.header.getBytes();
            currentWrite[i++] = batch.batch;
        }

        while (writeBatch()) {
            queued.drainTo(inFlight, maximumBatchedSize - inFlight.size());
            if (log.isTraceEnabled()) {
                log.trace(String.format("pushing %s batches", inFlight.size()));
            }
            if (inFlight.isEmpty()) {
                fsm.payloadWritten();
                return;
            }
            i = 0;
            for (Batch batch : inFlight) {
                currentWrite[i++] = batch.header.getBytes();
                currentWrite[i++] = batch.batch;
            }
        }

        if (inError) {
            fsm.close();
        } else {
            int added = queued.drainTo(inFlight,
                                       maximumBatchedSize - inFlight.size());
            if (added != 0) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("pushing additional %s batches",
                                            added));
                }
                int index = (inFlight.size() - added) * 2 - 1;
                i = 0;
                for (Batch batch : inFlight) {
                    if (i <= index) {
                        i += 2;
                        continue;
                    }
                    currentWrite[i++] = batch.header.getBytes();
                    currentWrite[i++] = batch.batch;
                }
                return;
            }
            handler.selectForWrite();
        }
    }

    protected void nextQuantum() {
        quantum.release();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        // assert sanityCheck();
        int offset = 0;
        for (; offset < inFlight.size(); offset++) {
            if (currentWrite[offset].hasRemaining()) {
                break;
            }
        }
        try {
            long written = handler.getChannel().write(currentWrite, offset,
                                                      inFlight.size());
            if (log.isTraceEnabled()) {
                log.trace(String.format("%s bytes written", written));
            }
            if (written < 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing channel");
                }
                inError = true;
                return false;
            } else if (currentWrite[inFlight.size() * 2 - 1].hasRemaining()) {
                for (; offset < inFlight.size(); offset++) {
                    if (currentWrite[offset].hasRemaining()) {
                        break;
                    }
                }
                // extra write attempt
                written = handler.getChannel().write(currentWrite, 0,
                                                     inFlight.size());
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
        if (!currentWrite[inFlight.size() * 2 - 1].hasRemaining()) {
            if (log.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder(4096);
                for (Batch batch : inFlight) {
                    builder.append('\n');
                    builder.append('\t');
                    builder.append(String.format("Pushed %s,%s,%s,%s",
                                                 batch.header.getSequenceNumber(),
                                                 batch.header.getChannel(),
                                                 fsm.getName(),
                                                 batch.header.getProducerMirror()));
                }
                log.info(builder.toString());
            }
            inFlight.clear();
            Arrays.fill(currentWrite, null);
            return true;
        }
        compactPushed();
        return !inFlight.isEmpty();
    }

    protected void writeReady() {
        fsm.writeReady();
    }
}
