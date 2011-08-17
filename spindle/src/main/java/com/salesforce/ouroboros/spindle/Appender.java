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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Appender {
    public enum State {
        ACCEPTED, APPEND, INITIALIZED, READ_HEADER;
    }

    private static final Logger   log   = LoggerFactory.getLogger(Appender.class);

    private final Bundle          bundle;
    private SocketChannelHandler  handler;
    private final EventHeader     header;
    private volatile long         offset;
    private volatile long         position;
    private volatile long         remaining;
    private volatile EventChannel eventChannel;
    private volatile Segment      segment;
    private volatile State        state = State.INITIALIZED;
    private final Producer        producer;

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
        state = State.ACCEPTED;
        this.handler = handler;
        this.handler.selectForRead();
    }

    public void handleRead(SocketChannel channel) {
        switch (state) {
            case ACCEPTED: {
                header.clear();
                state = State.READ_HEADER;
                readHeader(channel);
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
                log.error("Invalid read state: " + state);
            }
        }
        handler.selectForRead();
    }

    @Override
    public String toString() {
        return "Appender [state=" + state + ", segment=" + segment
               + ", remaining=" + remaining + ", position=" + position + "]";
    }

    private void append(SocketChannel channel) {
        long written;
        try {
            written = segment.transferFrom(channel, position, remaining);
        } catch (IOException e) {
            log.error("Exception during append", e);
            return;
        }
        position += written;
        remaining -= written;
        if (remaining == 0) {
            try {
                segment.position(position);
            } catch (IOException e) {
                log.error(String.format("Cannot determine position in segment: %s",
                                        segment), e);
            }
            segment = null;
            state = State.ACCEPTED;
            if (producer != null) {
                producer.commit(eventChannel, segment, offset, header);
            }
        }
    }

    private void readHeader(SocketChannel channel) {
        boolean read;
        try {
            read = header.read(channel);
        } catch (IOException e) {
            log.error("Exception during header read", e);
            return;
        }
        if (read) {
            try {
                eventChannel = bundle.eventChannelFor(header);
                segment = eventChannel.getAppendSegmentFor(header);
            } catch (IOException e) {
                return;
            }
            try {
                offset = position = segment.position();
            } catch (IOException e) {
                log.error(String.format("Cannot determine position in segment: %s",
                                        segment), e);
                return;
            }
            writeHeader();
            remaining = header.size();
            append(channel);
        }
    }

    private void writeHeader() {
        header.rewind();
        try {
            if (!header.write(segment)) {
                log.error(String.format("Unable to write complete header on: %s",
                                        segment));
            }
        } catch (IOException e) {
            log.error("Exception during header read", e);
            return;
        }
        position += Event.HEADER_BYTE_SIZE;
        state = State.APPEND;
    }
}