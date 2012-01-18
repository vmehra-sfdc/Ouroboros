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
package com.salesforce.ouroboros.spindle.source;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.spindle.source.AcknowledgerContext.AcknowledgerState;
import com.salesforce.ouroboros.util.lockfree.LockFreeQueue;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Acknowledger {

    static final Logger               log     = Logger.getLogger(Acknowledger.class.getCanonicalName());

    private final ByteBuffer          buffer  = ByteBuffer.allocate(BatchIdentity.BYTE_SIZE);
    private BatchIdentity             current;
    private final AcknowledgerContext fsm     = new AcknowledgerContext(this);
    private SocketChannelHandler      handler;
    private boolean                   inError;
    final Queue<BatchIdentity>        pending = new LockFreeQueue<BatchIdentity>();

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
        fsm.ack();
    }

    public void close() {
        handler.close();
        pending.clear();
    }

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
    }

    public AcknowledgerState getState() {
        return fsm.getState();
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private void error() {
        inError = true;
        current = null;
    }

    protected boolean inError() {
        return inError;
    }

    protected void processPendingAcks() {
        current = pending.poll();
        while (current != null) {
            buffer.rewind();
            current.serializeOn(buffer);
            if (!writeAck()) {
                if (inError) {
                    fsm.close();
                } else {
                    handler.selectForWrite();
                }
                return;
            }
            current = pending.poll();
        }
        fsm.waiting();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeAck() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write batch commit acknowledgement: %s",
                                  current), e);
            error();
            return false;
        }
        return !buffer.hasRemaining();
    }
}
