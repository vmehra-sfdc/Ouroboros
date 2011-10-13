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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * The state machine implementing the batch event acknowledgement protocol
 * 
 * @author hhildebrand
 * 
 */
public class BatchAcknowledgement {
    public enum State {
        CLOSED, ERROR, INITIALIZED, READ_ACK;
    };

    private final static Logger           log       = Logger.getLogger(BatchAcknowledgement.class.getCanonicalName());

    private final ByteBuffer              ackBuffer = ByteBuffer.allocate(BatchIdentity.BYTE_SIZE);
    private volatile SocketChannelHandler handler;
    private final Spinner                 spinner;
    private final AtomicReference<State>  state     = new AtomicReference<State>(
                                                                                 State.INITIALIZED);

    public BatchAcknowledgement(Spinner spinner) {
        this.spinner = spinner;
    }

    public void closing(SocketChannel channel) {
        if (state.get() != State.ERROR) {
            state.set(State.CLOSED);
        }
    }

    public State getState() {
        return state.get();
    }

    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        this.handler = handler;
        state.set(State.READ_ACK);
        handler.selectForRead();
    }

    public void handleRead(SocketChannel channel) {
        State s = state.get();
        switch (s) {
            case READ_ACK: {
                readAcknowlegement(channel);
            }
        }
    }

    private void error() {
        state.set(State.ERROR);
        handler.close();
    }

    private void readAcknowlegement(SocketChannel channel) {
        try {
            channel.read(ackBuffer);
        } catch (IOException e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING, "Error reading batch acknowlegement", e);
            }
            error();
        }
        if (!ackBuffer.hasRemaining()) {
            ackBuffer.flip();
            BatchIdentity ack = new BatchIdentity(ackBuffer);
            ackBuffer.rewind();
            spinner.acknowledge(ack);
            readAcknowlegement(channel);
        } else {
            handler.selectForRead();
        }
    }
}
