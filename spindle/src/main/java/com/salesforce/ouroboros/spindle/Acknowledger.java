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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Acknowledger {
    public enum State {
        ERROR, INTERRUPTED, PROCESSING, WAITING, WRITE_ACK;
    }

    private static final int                   POLL_TIMEOUT = 10;
    private static final TimeUnit              POLL_UNIT    = TimeUnit.MILLISECONDS;
    static final Logger                        log          = Logger.getLogger(Acknowledger.class.getCanonicalName());

    private final ByteBuffer                   buffer       = ByteBuffer.allocate(BatchIdentity.BYTE_SIZE);
    private volatile BatchIdentity             current;
    private volatile SocketChannelHandler      handler;
    private final BlockingQueue<BatchIdentity> pending      = new LinkedBlockingQueue<BatchIdentity>();
    private final AtomicReference<State>       state        = new AtomicReference<State>(
                                                                                         State.WAITING);

    /**
     * Replicate the event to the mirror
     * 
     * @param eventChannel
     * @param offset
     * @param segment
     * @param size
     */
    public void acknowledge(UUID channel, long timestamp) {
        pending.add(new BatchIdentity(channel, timestamp));
        process();
    }

    public State getState() {
        return state.get();
    }

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
    }

    public void writeReady() {
        switch (state.get()) {
            case WRITE_ACK: {
                writeAck();
                break;
            }
            default:
                log.warning(String.format("Illegal write state: %s", state));
        }
    }

    private void error() {
        state.set(State.ERROR);
        current = null;
        pending.clear();
    }

    private void process() {
        if (!state.compareAndSet(State.WAITING, State.PROCESSING)) {
            return;
        }
        BatchIdentity entry;
        try {
            entry = pending.poll(POLL_TIMEOUT, POLL_UNIT);
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

    private void process(BatchIdentity entry) {
        if (!state.compareAndSet(State.PROCESSING, State.WRITE_ACK)) {
            return;
        }
        current = entry;
        buffer.rewind();
        current.serializeOn(buffer);
        handler.selectForWrite();
    }

    private void writeAck() {
        try {
            handler.getChannel().write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write batch commit acknowledgement: %s",
                                  current), e);
            error();
            return;
        }
        if (buffer.hasRemaining()) {
            handler.selectForWrite();
        } else {
            state.set(State.WAITING);
            process();
        }
    }
}
