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
package com.salesforce.ouroboros.spindle;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * A duplicator of event streams. The duplicator provides outbound replication
 * of events sourced in the host process for a channel.
 * 
 * @author hhildebrand
 * 
 */
public final class Duplicator {
    public enum State {
        ERROR, INTERRUPTED, PROCESSING, WAITING, WRITE, WRITE_HEADER;
    }

    static final Logger                     log          = Logger.getLogger(Duplicator.class.getCanonicalName());
    private static final int                POLL_TIMEOUT = 10;
    private static final TimeUnit           POLL_UNIT    = TimeUnit.MILLISECONDS;

    private volatile EventEntry             current;
    private volatile SocketChannelHandler   handler;
    private final BlockingQueue<EventEntry> pending      = new LinkedBlockingQueue<EventEntry>();
    private volatile long                   position;
    private volatile int                    remaining;
    private final AtomicReference<State>    state        = new AtomicReference<State>(
                                                                                      State.WAITING);

    /**
     * @return the state of the outbound replicator
     */
    public State getState() {
        return state.get();
    }

    public void handleConnect(SocketChannel channel,
                              final SocketChannelHandler handler) {
        this.handler = handler;
    }

    public void handleWrite(SocketChannel channel) {
        switch (state.get()) {
            case WRITE_HEADER: {
                writeHeader(channel);
                break;
            }
            case WRITE: {
                writeBatch(channel);
                break;
            }
            default:
                log.warning(String.format("Illegal write state: %s", state));
        }
    }

    /**
     * Replicate the event to the mirror
     */
    public void replicate(ReplicatedBatchHeader header,
                          EventChannel eventChannel, Segment segment,
                          Acknowledger acknowledger) {
        pending.add(new EventEntry(header, eventChannel, segment, acknowledger));
        process();
    }

    private void error() {
        state.set(State.ERROR);
        if (current != null) {
            try {
                current.segment.close();
            } catch (IOException e1) {
                log.log(Level.FINEST, String.format("Error closing segment %s",
                                                    current.segment), e1);
            }
        }
        current = null;
        pending.clear();
    }

    private void process() {
        if (!state.compareAndSet(State.WAITING, State.PROCESSING)) {
            return;
        }
        EventEntry entry;
        try {
            entry = pending.poll(POLL_TIMEOUT, POLL_UNIT);
        } catch (InterruptedException e) {
            state.set(State.INTERRUPTED);
            return;
        }
        if (entry == null) {
            state.compareAndSet(State.PROCESSING, State.WAITING);
            return;
        }
        process(entry);
    }

    private void process(EventEntry entry) {
        if (!state.compareAndSet(State.PROCESSING, State.WRITE_HEADER)) {
            return;
        }
        current = entry;
        remaining = entry.header.getBatchByteLength();
        position = entry.header.getOffset();
        entry.header.rewind();
        handler.selectForWrite();
    }

    private boolean transferTo(SocketChannel channel) throws IOException {
        long p = position;
        int written = (int) current.segment.transferTo(p, remaining, channel);
        remaining = remaining - written;
        position = p + written;
        if (remaining == 0) {
            return true;
        }
        return false;
    }

    private void writeBatch(SocketChannel channel) {
        try {
            if (transferTo(channel)) {
                state.set(State.WAITING);
                current.eventChannel.commit(current.header.getOffset());
                current.acknowledger.acknowledge(current.header.getChannel(),
                                                 current.header.getTimestamp());
                process();
            }
        } catch (IOException e) {
            error();
            log.log(Level.WARNING,
                    String.format("Unable to replicate payload for event: %s from: %s",
                                  current.header.getOffset(), current.segment),
                    e);
        }
    }

    private void writeHeader(SocketChannel channel) {
        boolean written = false;
        try {
            written = current.header.write(channel);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write batch header: %s",
                                  current.header), e);
            error();
        }
        if (written) {
            state.set(State.WRITE);
            writeBatch(channel);
        } else {
            handler.selectForWrite();
        }
    }
}
