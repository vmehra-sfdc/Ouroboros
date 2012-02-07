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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.producer.spinner.BatchWriterContext.BatchWriterState;
import com.salesforce.ouroboros.util.RingBuffer;

/**
 * The event action context for the batch writing protocol FSM
 * 
 * @author hhildebrand
 * 
 */
public class BatchWriter {

    private static final Logger      log     = Logger.getLogger(BatchWriter.class.getCanonicalName());
    private final BatchWriterContext fsm     = new BatchWriterContext(this);
    private SocketChannelHandler     handler;
    private boolean                  inError = false;
    private final Queue<ByteBuffer>  queued;
    private ByteBuffer               batch;

    public BatchWriter(int maxQueueLength, String fsmName) {
        queued = new RingBuffer<ByteBuffer>(maxQueueLength);
        fsm.setName(fsmName);
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
        fsm.pushBatch(events, pending);
    }

    protected void close() {
        if (handler != null) {
            handler.close();
        }
        batch = null;
        queued.clear();
    }

    /**
     * @param batch
     * @param pending
     * @throws RateLimiteExceededException
     *             - if the event publishing rate has been exceeded
     */
    protected void enqueue(Batch batch, Map<BatchIdentity, Batch> pending) {
        if (!queued.offer(batch.batch)) {
            throw new RateLimiteExceededException(
                                                  "The maximum number of queued event batches has been exceeded");
        }
        pending.put(batch, batch);
    }

    protected boolean hasNext() {
        return !queued.isEmpty();
    }

    protected boolean hasPendingBatch() {
        return queued.poll() != null;
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
        batch = queued.remove();
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

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        try {
            if (handler.getChannel().write(batch) < 0) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write event batch payload %s",
                                      handler.getChannel()), e);
            }
            inError = true;
            return false;
        }
        return !batch.hasRemaining();
    }

    protected void writeReady() {
        fsm.writeReady();
    }
}
