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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;
import com.lmax.disruptor.RingBuffer;

/**
 * A full duplex replicator of event streams. The replicator provides both
 * outbound replication of events sourced in the host process as well as
 * accepting replicated events from partner processes on the same channel.
 * 
 * Replicators have a strict sense of connecting with their paired process. In
 * order to use both the inbound and outbound streams of the socket, each pair
 * of processes must only connect once. Thus, one process of the pair will
 * initiate the connection and the other pair will accept the new connection.
 * Once the replication connection is established, both sides will replicate
 * events between them.
 * 
 * @author hhildebrand
 * 
 */
public final class Replicator implements CommunicationsHandler {
    public enum State {
        WAITING, WRITE_HEADER, WRITE_PAYLOAD;
    }

    private static final Logger               log      = LoggerFactory.getLogger(Replicator.class);

    private volatile Appender                 appender;
    private final Bundle                      bundle;
    private final ConsumerBarrier<EventEntry> consumerBarrier;
    private final Executor                    executor;
    private volatile SocketChannelHandler     handler;
    private volatile EventHeader              header;
    private volatile long                     offset;
    private volatile int                      payloadRemaining;
    private volatile long                     position;
    private final AtomicBoolean               running  = new AtomicBoolean();
    private volatile Segment                  segment;
    private volatile long                     sequence = RingBuffer.INITIAL_CURSOR_VALUE;
    private volatile State                    state    = State.WAITING;

    public Replicator(final Bundle bundle,
                      final ConsumerBarrier<EventEntry> consumerBarrier,
                      final Executor executor) {
        this.bundle = bundle;
        this.consumerBarrier = consumerBarrier;
        this.executor = executor;
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    /**
     * @return the state of the inbound appender
     */
    public Appender.State getAppenderState() {
        return appender.getState();
    }

    /**
     * Get the {@link ConsumerBarrier} the {@link Consumer} is waiting on.
     * 
     * @return the barrier this {@link Consumer} is using.
     */
    public ConsumerBarrier<EventEntry> getConsumerBarrier() {
        return consumerBarrier;
    }

    /**
     * @return the state of the outbound replicator
     */
    public State getState() {
        return state;
    }

    /**
     * Halt the outbound replication process;
     */
    public void halt() {
        if (running.compareAndSet(true, false)) {
            consumerBarrier.alert();
        }
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        this.handler = handler;
        appender = new Appender(bundle, Producer.NULL_PRODUCER);
        run();
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              final SocketChannelHandler handler) {
        this.handler = handler;
        appender = new Appender(bundle, Producer.NULL_PRODUCER);
        run();
        appender.handleAccept(channel, handler);
    }

    @Override
    public void handleRead(SocketChannel channel) {
        appender.handleRead(channel);
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state) {
            case WRITE_HEADER: {
                try {
                    if (header.write(channel)) {
                        state = State.WRITE_PAYLOAD;
                    }
                } catch (IOException e) {
                    log.error(String.format("Unable to replicate header for event: %s from: %s",
                                            offset, segment), e);
                }
                break;
            }
            case WRITE_PAYLOAD: {
                try {
                    if (writePayload(channel)) {
                        state = State.WAITING;
                        evaluate();
                    }
                } catch (IOException e) {
                    log.error(String.format("Unable to replicate payload for event: %s from: %s",
                                            offset, segment), e);
                }
                break;
            }
            default:
                log.error("Illegal write state: " + state);
        }
    }

    private void evaluate() {
        if (running.get()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    processNext();
                }
            });
        }
    }

    private void processNext() {
        long nextSequence = sequence + 1;
        try {
            try {
                consumerBarrier.waitFor(nextSequence);
            } catch (InterruptedException e) {
                return;
            }
            EventEntry entry = consumerBarrier.getEntry(nextSequence);
            sequence = entry.getSequence();
            replicate(entry);
        } catch (final AlertException ex) {
            // Wake up from blocking wait
        }
    }

    private void replicate(EventEntry entry) {
        header = entry.getHeader().clone();
        offset = entry.getOffset();
        try {
            segment = bundle.segmentFor(offset, header);
        } catch (FileNotFoundException e1) {
            log.error(String.format("Unable to find segment for event: %s, offset: %s",
                                    header.getChannel(), offset));
        }
        payloadRemaining = header.size();
        position = offset + EventHeader.HEADER_BYTE_SIZE;
        header.rewind();
        try {
            seekToPayload();
        } catch (IOException e) {
            log.error(String.format("Unable to seek to event payload on: %s, offset: %s",
                                    segment, offset));
        }
        state = State.WRITE_HEADER;
        handler.selectForWrite();
    }

    private void run() {
        running.set(true);
        evaluate();
    }

    private void seekToPayload() throws IOException {
        header.seekToPayload(offset, segment);
    }

    private boolean writePayload(SocketChannel channel) throws IOException {
        int remaining = payloadRemaining;
        long p = position;
        int written = (int) segment.transferTo(p, remaining, channel);
        payloadRemaining = remaining - written;
        position = p + written;
        if (payloadRemaining == 0) {
            return true;
        }
        return false;
    }
}
