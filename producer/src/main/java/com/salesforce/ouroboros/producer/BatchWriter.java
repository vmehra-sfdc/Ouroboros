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
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.EventHeader;

/**
 * The state machine for handling non blocking writes of event batches.
 * 
 * @author hhildebrand
 * 
 */
public class BatchWriter {
    public enum State {
        CLOSED, ERROR, FAILOVER, INITIALIZED, INTERRUPTED, PROCESSING, WAITING,
        WRITE_BATCH_HEADER, WRITE_EVENT_HEADER, WRITE_PAYLOAD;
    }

    private static final Logger           log          = Logger.getLogger(BatchWriter.class.getCanonicalName());
    private static final int              MAGIC        = 0x1638;
    private static final int              POLL_TIMEOUT = 10;
    private static final TimeUnit         POLL_UNIT    = TimeUnit.MILLISECONDS;

    private final Deque<ByteBuffer>       batch        = new LinkedList<ByteBuffer>();
    private final BatchHeader             batchHeader  = new BatchHeader();
    private volatile SocketChannelHandler handler;
    private final EventHeader             header       = new EventHeader();
    private final BlockingQueue<Batch>    queued       = new LinkedBlockingQueue<Batch>();
    private final AtomicReference<State>  state        = new AtomicReference<State>(
                                                                                    State.INITIALIZED);

    public void closing(SocketChannel channel) {
        final State s = state.get();
        if (s != State.ERROR) {
            state.set(State.CLOSED);
        }
        queued.clear();
        batch.clear();
    }

    public void failover() {
        state.set(State.FAILOVER);
    }

    public State getState() {
        return state.get();
    }

    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        this.handler = handler;
        state.set(State.WAITING);
        process();
    }

    public void handleWrite(SocketChannel channel) {
        final State s = state.get();
        switch (s) {
            case WRITE_BATCH_HEADER:
                writeBatchHeader(channel);
                break;
            case WRITE_PAYLOAD:
                writePayload(channel);
                break;
            case WRITE_EVENT_HEADER:
                writeEventHeader(channel);
                break;
            case FAILOVER:
                break;
            default:
                state.set(State.ERROR);
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Illegal write state: %s", s));
                }
        }
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
        State s = state.get();
        if (s == State.CLOSED || s == State.ERROR) {
            return false;
        }
        pending.put(events, events);
        queued.add(events);
        process();
        return true;
    }

    /**
     * Batch the events to the channel buffer
     * 
     * @param channel
     *            - the unique id of the channel
     * @param timestamp
     *            - the timestamp used for dedup
     * @param events
     *            - the event payloads to batch
     */
    private void batch(Batch entry) {
        if (!state.compareAndSet(State.PROCESSING, State.WRITE_BATCH_HEADER)) {
            throw new IllegalStateException(
                                            String.format("Cannot batch events in state %s",
                                                          state.get()));
        }
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

    private void error() {
        handler.close();
        state.set(State.ERROR);
        batch.clear();
        queued.clear();
    }

    /**
     * Process a pending event batch, if available
     */
    private void process() {
        if (!state.compareAndSet(State.WAITING, State.PROCESSING)) {
            return;
        }
        Batch entry;
        try {
            entry = queued.poll(POLL_TIMEOUT, POLL_UNIT);
        } catch (InterruptedException e) {
            state.set(State.INTERRUPTED);
            return;
        }
        if (entry == null) {
            state.compareAndSet(State.PROCESSING, State.WAITING);
            return;
        }
        batch(entry);
    }

    private void writeBatchHeader(SocketChannel channel) {
        try {
            batchHeader.write(channel);
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write batch header %s on %s",
                                      batchHeader, channel), e);
            }
            error();
            return;
        }
        if (!batchHeader.hasRemaining()) {
            writeNextEventHeader(channel);
        } else {
            handler.selectForWrite();
        }
    }

    private void writeEventHeader(SocketChannel channel) {
        try {
            header.write(channel);
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write header %s on %s",
                                      header, channel), e);
            }
            error();
            return;
        }
        if (!header.hasRemaining()) {
            state.set(State.WRITE_PAYLOAD);
            writePayload(channel);
        } else {
            handler.selectForWrite();
        }
    }

    private void writeNextEventHeader(SocketChannel channel) {
        state.set(State.WRITE_EVENT_HEADER);
        header.initialize(MAGIC, batch.peekFirst());
        header.rewind();
        writeEventHeader(channel);
    }

    private void writePayload(SocketChannel channel) {
        try {
            channel.write(batch.peekFirst());
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write event batch", channel),
                        e);
            }
            error();
            return;
        }
        if (!batch.peekFirst().hasRemaining()) {
            batch.pop();
            if (batch.isEmpty()) {
                state.set(State.WAITING);
                process();
            } else {
                writeNextEventHeader(channel);
            }
        } else {
            handler.selectForWrite();
        }
    }
}
