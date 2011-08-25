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
public class XeroxMachine implements CommunicationsHandler {
    private static final Logger log   = Logger.getLogger(XeroxMachine.class.getCanonicalName());
    public static final long    MAGIC = 0x1638L;

    private enum State {
        INITIALIZED, HANDSHAKE, COPY, ERROR;
    };

    private final UUID                    channelId;
    private final ByteBuffer              buffer;
    private final Deque<Segment>          segments;
    private volatile Segment              current;
    private volatile State                state;
    private volatile SocketChannelHandler handler;

    public XeroxMachine(UUID id, EventChannel channel, int bufferSize) {
        channelId = id;
        buffer = ByteBuffer.allocate(bufferSize);
        segments = channel.getSegmentStack();
        state = State.INITIALIZED;
    }

    @Override
    public void closing(SocketChannel channel) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleConnect(SocketChannel channel, SocketChannelHandler h) {
        handler = h;
        switch (state) {
            case INITIALIZED: {
                state = State.HANDSHAKE;
                buffer.putLong(MAGIC);
                buffer.putLong(channelId.getLeastSignificantBits());
                buffer.putLong(channelId.getMostSignificantBits());
                buffer.flip();
                handshake(channel);
                break;
            }
            default: {
                log.warning(String.format("Illegal connect state: %s", state));
            }
        }
    }

    private void handshake(SocketChannel channel) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing handshake for %s on %s",
                                  channelId, channel), e);
            state = State.ERROR;
            try {
                channel.close();
            } catch (IOException e1) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", channel), e1);
            }
        }
        if (!buffer.hasRemaining()) {
            buffer.clear();
            state = State.COPY;
        }
        handler.selectForWrite();
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
            case COPY: {
                copy(channel);
                break;
            }
        }
    }

    private void copy(SocketChannel channel) {

    }

}
