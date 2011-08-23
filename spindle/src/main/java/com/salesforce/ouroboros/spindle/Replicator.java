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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * A replicator of the event stream of a channel.
 * 
 * @author hhildebrand
 * 
 */
public final class Replicator implements CommunicationsHandler {
    public enum State {
        INTERRUPTED, PROCESSING, WAITING, WRITE, WRITE_OFFSET;
    }

    static final Logger                     log          = Logger.getLogger(Replicator.class.getCanonicalName());

    private volatile SocketChannelHandler   handler;
    private volatile long                   offset;
    private final ByteBuffer                offsetBuffer = ByteBuffer.allocate(8);
    private final BlockingQueue<EventEntry> pending      = new LinkedBlockingQueue<EventEntry>();
    private volatile long                   position;
    private volatile int                    remaining;
    private volatile Segment                segment;
    private final AtomicReference<State>    state        = new AtomicReference<State>(
                                                                                      State.WAITING);
    final EventChannel                      eventChannel;

    public Replicator(final EventChannel eventChannel) {
        this.eventChannel = eventChannel;
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    /**
     * @return the state of the outbound replicator
     */
    public State getState() {
        return state.get();
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException(
                                                "Inbound channels are not supported");
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              final SocketChannelHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handleRead(SocketChannel channel) {
        throw new UnsupportedOperationException(
                                                "Inbound channels are not supported");
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state.get()) {
            case WRITE_OFFSET: {
                writeOffset(channel);
                break;
            }
            case WRITE: {
                writeEvent(channel);
                break;
            }
            default:
                log.warning(String.format("Illegal write state: %s", state));
        }
    }

    public void replicate(long offset, Segment segment, int size) {
        pending.add(new EventEntry(offset, segment, size));
        process();
    }

    private boolean transferTo(SocketChannel channel) throws IOException {
        long p = position;
        int written = (int) segment.transferTo(p, remaining, channel);
        remaining = remaining - written;
        position = p + written;
        if (remaining == 0) {
            return true;
        }
        return false;
    }

    private void writeEvent(SocketChannel channel) {
        try {
            if (transferTo(channel)) {
                state.set(State.WAITING);
                eventChannel.commit(offset);
                process();
            }
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to replicate payload for event: %s from: %s",
                                  offset, segment), e);
        }
    }

    private void writeOffset(SocketChannel channel) {
        try {
            channel.write(offsetBuffer);
        } catch (IOException e) {
            log.log(Level.WARNING, "Error writing offset", e);
        }
        if (!offsetBuffer.hasRemaining()) {
            state.set(State.WRITE);
            writeEvent(channel);
        }
    }

    protected void process() {
        if (!state.compareAndSet(State.WAITING, State.PROCESSING)) {
            return;
        }
        EventEntry entry;
        try {
            entry = pending.poll(10, TimeUnit.MILLISECONDS);
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

    protected void process(EventEntry entry) {
        if (!state.compareAndSet(State.PROCESSING, State.WRITE_OFFSET)) {
            return;
        }
        offset = entry.offset;
        segment = entry.segment;
        remaining = entry.size;
        position = offset;
        offsetBuffer.clear();
        offsetBuffer.putLong(offset);
        offsetBuffer.flip();
        handler.selectForWrite();
    }
}
