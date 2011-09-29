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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Event;

/**
 * The state machine for handling non blocking writes of event batches.
 * 
 * @author hhildebrand
 * 
 */
public class Spinner {
    public enum State {
        CLOSED, ERROR, READY, WRITE_EVENT, WRITE_SIZE;
    }

    private static Logger                                                  log        = Logger.getLogger(Spinner.class.getCanonicalName());

    private final Deque<Event>                                             events     = new LinkedList<Event>();
    private volatile SocketChannelHandler<? extends CommunicationsHandler> handler;
    private final ByteBuffer                                               sizeBuffer = ByteBuffer.allocate(4);
    private final AtomicReference<State>                                   state      = new AtomicReference<State>();

    public void closing(SocketChannel channel) {
        if (state.get() != State.ERROR) {
            state.set(State.CLOSED);
        }
    }

    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler<? extends CommunicationsHandler> handler) {
        this.handler = handler;
    }

    public void handleWrite(SocketChannel channel) {
        final State s = state.get();
        switch (s) {
            case WRITE_SIZE:
                writeSize(channel);
                break;
            case WRITE_EVENT:
                writeEvent(channel);
                break;
            default:
                state.set(State.ERROR);
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Illegal write state: %s", s));
                }
        }
    }

    private void error() {
        handler.close();
        state.set(State.ERROR);
        events.clear();
    }

    private void writeEvent(SocketChannel channel) {
        Event event = events.peekFirst();
        assert event != null : "Illegal state: events are empty";
        boolean written = false;
        try {
            written = event.write(channel);
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write event", channel), e);
            }
            error();
            return;
        }
        if (written) {
            events.pop();
            if (events.isEmpty()) {
                state.set(State.READY);
            } else {
                // Try for another write
                writeEvent(channel);
            }
            return;
        }
        handler.selectForWrite();
    }

    private void writeSize(SocketChannel channel) {
        try {
            channel.write(sizeBuffer);
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Unable to write events size", channel),
                        e);
            }
            error();
            return;
        }
        if (!sizeBuffer.hasRemaining()) {
            state.set(State.WRITE_EVENT);
            writeEvent(channel);
        }
    }

}
