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
package com.salesforce.ouroboros.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.producer.BatchWriterContext.BatchWriterFSM;
import com.salesforce.ouroboros.producer.BatchWriterContext.BatchWriterState;

/**
 * The event action context for the batch writing protocol FSM
 * 
 * @author hhildebrand
 * 
 */
public class BatchWriter {

    private static final Logger           log         = Logger.getLogger(BatchWriter.class.getCanonicalName());
    private static final int              MAGIC       = 0x1638;

    private final BatchHeader             batchHeader = new BatchHeader();
    private final BatchWriterContext      fsm         = new BatchWriterContext(
                                                                               this);
    private volatile SocketChannelHandler handler;
    private final EventHeader             header      = new EventHeader();
    private boolean                       inError     = false;
    private final BlockingQueue<Batch>    queued      = new LinkedBlockingQueue<Batch>();
    final Deque<ByteBuffer>               batch       = new LinkedList<ByteBuffer>();

    public void closing() {
        if (!fsm.isInTransition()) {
            fsm.close();
        }
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
     * @return true if the events have been scheduled for push, false if the
     *         receiver cannot push the events
     */
    public boolean push(Batch events, Map<BatchIdentity, Batch> pending) {
        pending.put(events, events);
        queued.add(events);
        if (!fsm.isInTransition()) {
            fsm.pushBatch();
        }
        if (fsm.getState() == BatchWriterFSM.Closed) {
            queued.clear();
            pending.remove(events);
            return false;
        } else {
            return true;
        }
    }

    protected void batch(Batch entry, Map<BatchIdentity, Batch> pending) {
        pending.put(entry, entry);
        int totalSize = 0;
        for (ByteBuffer event : entry.events) {
            totalSize += EventHeader.HEADER_BYTE_SIZE + event.remaining();
            batch.add(event);
        }
        batchHeader.initialize(entry.mirror, totalSize, MAGIC, entry.channel,
                               entry.timestamp);
        batchHeader.rewind();
        handler.selectForWrite();
    }

    protected void close() {
        handler.close();
        batch.clear();
        queued.clear();
    }

    protected boolean hasPendingBatch() {
        return queued.poll() != null;
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
        Batch entry;
        try {
            entry = queued.take();
        } catch (InterruptedException e) {
            inError = true;
            return;
        }
        int totalSize = 0;
        for (ByteBuffer event : entry.events) {
            totalSize += EventHeader.HEADER_BYTE_SIZE + event.remaining();
            batch.add(event);
        }
        batchHeader.initialize(entry.mirror, totalSize, MAGIC, entry.channel,
                               entry.timestamp);
        batchHeader.rewind();
        if (writeBatchHeader()) {
            fsm.batchHeaderWritten();
        } else {
            handler.selectForWrite();
        }
    }

    protected void nextEventHeader() {
        header.initialize(MAGIC, batch.peekFirst());
        header.rewind();
        if (writeEventHeader()) {
            fsm.eventHeaderWritten();
        } else {
            handler.selectForWrite();
        }
    }

    protected void nextPayload() {
        if (writePayload()) {
            if (batch.isEmpty()) {
                fsm.waiting();
            } else {
                fsm.payloadWritten();
            }
        } else {
            handler.selectForWrite();
        }
    }

    protected boolean payloadWritten() {
        return !batch.peekFirst().hasRemaining();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatchHeader() {
        try {
            batchHeader.write(handler.getChannel());
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write batch header %s on %s",
                                      batchHeader, handler.getChannel()), e);
            }
            inError = true;
            return false;
        }
        return !batchHeader.hasRemaining();
    }

    protected boolean writeEventHeader() {
        try {
            header.write(handler.getChannel());
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write header %s on %s",
                                      header, handler.getChannel()), e);
            }
            inError = true;
            return false;
        }
        return !header.hasRemaining();
    }

    protected boolean writePayload() {
        try {
            handler.getChannel().write(batch.peekFirst());
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write event batch payload %s",
                                      handler.getChannel()), e);
            }
            inError = true;
            return false;
        }
        if (!batch.peekFirst().hasRemaining()) {
            batch.pop();
            return true;
        }
        return false;
    }

    protected void writeReady() {
        fsm.writeReady();
    }
}
