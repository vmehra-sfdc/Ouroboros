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
import java.util.Deque;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * This class duplicates a channel state from the primary to the secondary.
 * 
 * @author hhildebrand
 * 
 */
public class Xerox implements CommunicationsHandler {

    public enum State {
        COPY, ERROR, FINISHED, HANDSHAKE, INITIALIZED, SEND_CLOSE, SEND_HEADER;
    }

    public static final long                 MAGIC             = 0x1638L;
    private static final int                 BUFFER_SIZE       = 8 + 8 + 8;
    private static final Logger              log               = Logger.getLogger(Xerox.class.getCanonicalName());
    private static final int                 DEFAULT_TXFR_SIZE = 16 * 1024;

    private final ByteBuffer                 buffer            = ByteBuffer.allocate(BUFFER_SIZE);
    private final EventChannel               eventChannel;
    private volatile Segment                 current;
    private volatile SocketChannelHandler<?> handler;
    private volatile long                    position;
    private final Deque<Segment>             segments;
    private volatile long                    segmentSize;
    private volatile State                   state;
    private final long                       transferSize;
    private final Node                       node;

    public Xerox(Node toNode, EventChannel channel) {
        this(toNode, channel, DEFAULT_TXFR_SIZE);
    }

    public Xerox(Node toNode, EventChannel channel, int transferSize) {
        node = toNode;
        eventChannel = channel;
        this.transferSize = transferSize;
        segments = channel.getSegmentStack();
        state = State.INITIALIZED;
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    /**
     * @return the node
     */
    public Node getNode() {
        return node;
    }

    public State getState() {
        return state;
    }

    @Override
    public void handleAccept(SocketChannel channel,
                             SocketChannelHandler<?> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleConnect(SocketChannel channel, SocketChannelHandler<?> h) {
        handler = h;
        switch (state) {
            case INITIALIZED: {
                state = State.HANDSHAKE;
                UUID channelId = eventChannel.getId();
                buffer.putLong(MAGIC);
                buffer.putLong(channelId.getMostSignificantBits());
                buffer.putLong(channelId.getLeastSignificantBits());
                buffer.flip();
                handshake(channel);
                break;
            }
            default: {
                log.warning(String.format("Illegal connect state: %s", state));
            }
        }
    }

    @Override
    public void handleRead(SocketChannel channel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state) {
            case HANDSHAKE: {
                handshake(channel);
                break;
            }
            case SEND_HEADER: {
                sendHeader(channel);
                break;
            }
            case COPY: {
                copy(channel);
                break;
            }
            default: {
                log.warning(String.format("Illegal write state: ", state));
            }
        }
    }

    private void copy(SocketChannel channel) {
        long written;
        try {
            written = current.transferTo(position, transferSize, channel);
        } catch (IOException e) {
            log.log(Level.WARNING, String.format("Error transfering %s on %s",
                                                 current, channel), e);
            terminate(channel);
            return;
        }
        position += written;
        if (position == segmentSize) {
            try {
                current.close();
            } catch (IOException e) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", current), e);
            }
            state = State.SEND_HEADER;
            sendNextSegment(channel);
        }
    }

    private void handshake(SocketChannel channel) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing handshake for %s on %s",
                                  eventChannel.getId(), channel), e);
            terminate(channel);
            return;
        }
        if (!buffer.hasRemaining()) {
            buffer.clear();
            state = State.SEND_HEADER;
            sendNextSegment(channel);
        }
        handler.selectForWrite();
    }

    private void sendHeader(SocketChannel channel) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing header for %s on %s", current,
                                  channel), e);
            terminate(channel);
            return;
        }
        if (!buffer.hasRemaining()) {
            state = State.COPY;
            copy(channel);
        }
        handler.selectForWrite();
    }

    private void sendNextSegment(SocketChannel channel) {
        state = State.SEND_HEADER;
        position = 0;
        if (segments.isEmpty()) {
            state = State.FINISHED;
            return;
        }
        current = segments.pop();
        try {
            segmentSize = current.size();
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error retrieving size of %s", current), e);
            terminate(channel);
            return;
        }
        buffer.clear();
        buffer.putLong(MAGIC);
        buffer.putLong(current.getPrefix());
        buffer.putLong(segmentSize);
        buffer.flip();
        sendHeader(channel);
    }

    private void terminate(SocketChannel channel) {
        state = State.ERROR;
        try {
            channel.close();
        } catch (IOException e1) {
            log.log(Level.FINE, String.format("Error closing: %s", channel), e1);
        }
        for (Segment segment : segments) {
            try {
                segment.close();
            } catch (IOException e1) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", segment), e1);
            }
        }
    }
}
