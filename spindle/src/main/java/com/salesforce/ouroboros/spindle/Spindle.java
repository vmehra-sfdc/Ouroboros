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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;

/**
 * The communications wrapper that ties together the appender and the
 * acknowledger.
 * 
 * @author hhildebrand
 * 
 */
public class Spindle implements CommunicationsHandler {

    public enum State {
        ERROR, ESTABLISHED, INITIAL;
    }

    public static final Integer          HANDSHAKE_SIZE = Node.BYTE_LENGTH + 4;
    public final static int              MAGIC          = 0x1638;
    private final static Logger          log            = Logger.getLogger(Spindle.class.getCanonicalName());

    private final Bundle                 bundle;
    private SocketChannelHandler         handler;
    private ByteBuffer                   handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private final AtomicReference<State> state          = new AtomicReference<State>(
                                                                                     State.INITIAL);
    final Acknowledger                   acknowledger;
    final Appender                       appender;

    public Spindle(Bundle bundle) {
        acknowledger = new Acknowledger();
        appender = new Appender(bundle, acknowledger);
        this.bundle = bundle;
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    public State getState() {
        return state.get();
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        this.handler = handler;
        acknowledger.handleAccept(channel, handler);
        appender.handleAccept(channel, handler);
        readHandshake(channel);
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleRead(SocketChannel channel) {
        final State s = state.get();
        switch (s) {
            case INITIAL: {
                readHandshake(channel);
                break;
            }
            case ESTABLISHED: {
                appender.handleRead(channel);
                break;
            }
            default:
                state.set(State.ERROR);
                log.warning(String.format("Invalid state for read: %s", s));
        }
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        acknowledger.handleWrite(channel);
    }

    private void readHandshake(SocketChannel channel) {
        try {
            channel.read(handshake);
        } catch (IOException e) {
            state.set(State.ERROR);
            log.log(Level.WARNING, String.format("Error reading handshake"), e);
            handler.close();
        }
        if (!handshake.hasRemaining()) {
            handshake.flip();
            int magic = handshake.getInt();
            if (magic != MAGIC) {
                state.set(State.ERROR);
                log.warning(String.format("Invalid handshak magic: %s", magic));
                handler.close();
                handshake = null;
                return;
            }
            Node producer = new Node(handshake);
            handshake = null;
            bundle.map(producer, acknowledger);
            state.set(State.ESTABLISHED);
        }
        handler.selectForRead();
    }
}