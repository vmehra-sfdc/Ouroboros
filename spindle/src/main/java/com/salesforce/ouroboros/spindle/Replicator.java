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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;
import com.lmax.disruptor.RingBuffer;

/**
 * A full duplex replicator of event streams. The replicator provides both
 * outbound replication of events sourced in the host process as well as
 * accepting replicated events from the mirrored partner process on the same
 * channel.
 * 
 * Replicators have a strict sense of connecting with their mirror process. In
 * order to use both the inbound and outbound streams of the socket, each pair
 * of processes must only connect once. Thus, one process of the mirror pair
 * will initiate the connection and the other pair will accept the new
 * connection. Once the replication connection is established, both sides will
 * replicate events between them.
 * 
 * @author hhildebrand
 * 
 */
public final class Replicator implements CommunicationsHandler, Producer,
        Consumer {
    public static class ReplicatingAppender extends Appender {
        private static final Logger log          = Logger.getLogger(ReplicatingAppender.class.getCanonicalName());
        private final ByteBuffer    offsetBuffer = ByteBuffer.allocate(8);

        public ReplicatingAppender(Bundle bundle, Producer producer) {
            super(bundle, producer);
        }

        /* (non-Javadoc)
         * @see com.salesforce.ouroboros.spindle.Appender#initialRead(java.nio.channels.SocketChannel)
         */
        @Override
        protected void initialRead(SocketChannel channel) {
            state = State.READ_OFFSET;
            offsetBuffer.clear();
            readOffset(channel);
        }

        /* (non-Javadoc)
         * @see com.salesforce.ouroboros.spindle.Appender#readHeader(java.nio.channels.SocketChannel)
         */
        @Override
        protected void readHeader(SocketChannel channel) {
            boolean read;
            try {
                read = header.read(channel);
            } catch (IOException e) {
                log.log(Level.WARNING, "Exception during header read", e);
                return;
            }
            if (read) {
                if (log.isLoggable(Level.FINER)) {
                    log.finer(String.format("Header read, header=%s", header));
                }
                eventChannel = bundle.eventChannelFor(header);
                if (eventChannel == null) {
                    log.info(String.format("No existing event channel for: %s",
                                           header));
                    state = State.DEV_NULL;
                    segment = null;
                    devNull = ByteBuffer.allocate(header.size());
                    devNull(channel);
                    return;
                }
                segment = eventChannel.segmentFor(offset);
                remaining = header.size();
                if (!eventChannel.isNextAppend(offset)) {
                    state = State.DEV_NULL;
                    segment = null;
                    devNull = ByteBuffer.allocate(header.size());
                    devNull(channel);
                } else {
                    writeHeader();
                    append(channel);
                }
            }
        }

        /* (non-Javadoc)
         * @see com.salesforce.ouroboros.spindle.Appender#readOffset(java.nio.channels.SocketChannel)
         */
        @Override
        protected void readOffset(SocketChannel channel) {
            try {
                channel.read(offsetBuffer);
            } catch (IOException e) {
                log.log(Level.WARNING, "Exception during offset read", e);
                return;
            }
            if (!offsetBuffer.hasRemaining()) {
                offsetBuffer.flip();
                offset = position = offsetBuffer.getLong();
                if (log.isLoggable(Level.FINER)) {
                    log.finer(String.format("Offset read, offset=%s", offset));
                }
                state = State.READ_HEADER;
                readHeader(channel);
            }
        }

    }

    public enum State {
        WAITING, WRITE, WRITE_OFFSET;
    }

    private static final Logger               log          = Logger.getLogger(Replicator.class.getCanonicalName());

    private final ReplicatingAppender         appender;
    private volatile EventChannel             eventChannel;
    private final Executor                    executor;
    private volatile SocketChannelHandler     handler;
    private volatile long                     offset;
    private final ByteBuffer                  offsetBuffer = ByteBuffer.allocate(8);
    private volatile long                     position;
    private volatile int                      remaining;
    private final ConsumerBarrier<EventEntry> replicationConsumerBarrier;
    private final AtomicBoolean               running      = new AtomicBoolean();
    private volatile Segment                  segment;
    private volatile long                     sequence     = RingBuffer.INITIAL_CURSOR_VALUE;
    private volatile State                    state        = State.WAITING;

    public Replicator(final Bundle bundle,
                      final ConsumerBarrier<EventEntry> replicationConsumerBarrier) {
        this.replicationConsumerBarrier = replicationConsumerBarrier;
        executor = Executors.newSingleThreadExecutor();
        appender = new ReplicatingAppender(bundle, this);
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    @Override
    public void commit(EventChannel channel, Segment segment, long offset,
                       EventHeader header) {
        channel.append(offset, header);
        try {
            segment.force(false);
        } catch (IOException e) {
            log.log(Level.SEVERE,
                    String.format("Unable to force segment: %s", segment), e);
        }
        try {
            segment.close();
        } catch (IOException e) {
            log.log(Level.INFO,
                    String.format("Unable to close segment: %s", segment), e);
        }
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
        return replicationConsumerBarrier;
    }

    @Override
    public long getSequence() {
        return sequence;
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
    @Override
    public void halt() {
        if (running.compareAndSet(true, false)) {
            replicationConsumerBarrier.alert();
        }
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        this.handler = handler;
        appender.handleAccept(channel, handler);
        run();
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              final SocketChannelHandler handler) {
        this.handler = handler;
        appender.handleAccept(channel, handler);
        run();
    }

    @Override
    public void handleRead(SocketChannel channel) {
        appender.handleRead(channel);
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state) {
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

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        evaluate();
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
                replicationConsumerBarrier.waitFor(nextSequence);
            } catch (InterruptedException e) {
                return;
            }
            EventEntry entry = replicationConsumerBarrier.getEntry(nextSequence);
            sequence = entry.getSequence();
            replicate(entry);
        } catch (final AlertException ex) {
            // Wake up from blocking wait
        }
    }

    private void replicate(EventEntry entry) {
        offset = entry.getOffset();
        segment = entry.getSegment();
        remaining = entry.getSize();
        eventChannel = entry.getChannel();
        entry.clear();
        position = offset;
        offsetBuffer.clear();
        offsetBuffer.putLong(offset);
        offsetBuffer.flip();
        state = State.WRITE_OFFSET;
        handler.selectForWrite();
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
                state = State.WAITING;
                eventChannel.commit(offset);
                evaluate();
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
            state = State.WRITE;
            writeEvent(channel);
        }
    }
}
