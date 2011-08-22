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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * The asynchronous appender of events. Instances of this class are responsible
 * for accepting inbound events from the socket channel and appending them to
 * the appropriate segment of the event channel to which they belong.
 * 
 * @author hhildebrand
 * 
 */
public class Appender {
    public enum State {
        ACCEPTED, APPEND, INITIALIZED, READ_HEADER, READ_OFFSET, DEV_NULL;
    }

    private static final Logger     log      = Logger.getLogger(Appender.class.getCanonicalName());

    protected final Bundle          bundle;
    protected SocketChannelHandler  handler;
    protected final EventHeader     header;
    protected volatile long         offset;
    protected volatile long         position = -1L;
    protected volatile long         remaining;
    protected volatile EventChannel eventChannel;
    protected volatile Segment      segment;
    protected volatile State        state    = State.INITIALIZED;
    protected final Producer        producer;
    protected volatile ByteBuffer   devNull;

    public Appender(Bundle bundle, Producer producer) {
        this.producer = producer;
        this.bundle = bundle;
        header = new EventHeader(
                                 ByteBuffer.allocate(EventHeader.HEADER_BYTE_SIZE));
    }

    public State getState() {
        return state;
    }

    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        assert state == State.INITIALIZED;
        if (log.isLoggable(Level.FINER)) {
            log.finer("ACCEPT");
        }
        state = State.ACCEPTED;
        this.handler = handler;
        this.handler.selectForRead();
    }

    public void handleRead(SocketChannel channel) {
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("READ, state=%s", state));
        }
        switch (state) {
            case ACCEPTED: {
                initialRead(channel);
                break;
            }
            case DEV_NULL: {
                devNull(channel);
                break;
            }
            case READ_OFFSET: {
                readOffset(channel);
                break;
            }
            case READ_HEADER: {
                readHeader(channel);
                break;
            }
            case APPEND: {
                append(channel);
                break;
            }
            default: {
                log.severe(String.format("Invalid read state: %s", state));
            }
        }
        handler.selectForRead();
    }

    @Override
    public String toString() {
        return "Appender [state=" + state + ", segment=" + segment
               + ", remaining=" + remaining + ", position=" + position + "]";
    }

    protected void append(SocketChannel channel) {
        long written;
        try {
            written = segment.transferFrom(channel, position, remaining);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during append", e);
            return;
        }
        position += written;
        remaining -= written;
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Appending, position=%s, remaining=%s, written=%s",
                                   position, remaining, written));
        }
        if (remaining == 0) {
            try {
                segment.position(position);
            } catch (IOException e) {
                log.log(Level.SEVERE,
                        String.format("Cannot determine position in segment: %s",
                                      segment), e);
            }
            if (producer != null) {
                producer.commit(eventChannel, segment, offset, header);
            }
            segment = null;
            state = State.ACCEPTED;
        }
    }

    protected void devNull(SocketChannel channel) {
        long read;
        try {
            read = channel.read(devNull);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during append", e);
            return;
        }
        position += read;
        remaining -= read;
        if (remaining == 0) {
            devNull = null;
            state = State.ACCEPTED;
        }
    }

    protected void initialRead(SocketChannel channel) {
        header.clear();
        state = State.READ_HEADER;
        readHeader(channel);
    }

    protected void readHeader(SocketChannel channel) {
        boolean read;
        try {
            read = header.read(channel);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during header read", e);
            return;
        }
        if (read) {
            eventChannel = bundle.eventChannelFor(header);
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Header read, header=%s", header));
            }
            if (eventChannel == null) {
                log.warning(String.format("No existing event channel for: %s",
                                          header));
                state = State.DEV_NULL;
                segment = null;
                devNull = ByteBuffer.allocate(header.size());
                devNull(channel);
                return;
            }
            segment = eventChannel.appendSegmentFor(header);
            offset = position = eventChannel.nextOffset();
            remaining = header.size();
            if (eventChannel.isDuplicate(header)) {
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

    protected void readOffset(SocketChannel channel) {
        // Do nothing for the basic appender
    }

    protected void writeHeader() {
        header.rewind();
        try {
            if (!header.write(segment)) {
                log.log(Level.SEVERE,
                        String.format("Unable to write complete header on: %s",
                                      segment));
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during header read", e);
            return;
        }
        position += EventHeader.HEADER_BYTE_SIZE;
        state = State.APPEND;
    }
}