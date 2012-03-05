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
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.source.AcknowledgerContext.AcknowledgerState;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Acknowledger {

    static final Logger                log     = Logger.getLogger(Acknowledger.class.getCanonicalName());

    private ByteBuffer                 buffer;
    private final AcknowledgerContext  fsm     = new AcknowledgerContext(this);
    private SocketChannelHandler       handler;
    private boolean                    inError;
    private final Queue<BatchIdentity> pending = new LinkedList<BatchIdentity>();
    private final Bundle               bundle;
    private Node                       producer;

    public Acknowledger(Bundle bundle) {
        this.bundle = bundle;
    }

    /**
     * Replicate the event to the mirror
     * 
     * @param eventChannel
     * @param offset
     * @param segment
     * @param size
     */
    public void acknowledge(UUID channel, long sequenceNumber) {
        fsm.ack(channel, sequenceNumber);
    }

    public void close() {
        handler.close();
    }

    public void closing() {
        pending.clear();
        bundle.closeAcknowledger(producer);
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
        buffer = null;
    }

    protected boolean inError() {
        return inError;
    }

    protected void processPendingAcks() {
        if (pending.isEmpty()) {
            return;
        }
        int batchByteSize = pending.size() * BatchHeader.HEADER_BYTE_SIZE;
        if (buffer != null && buffer.capacity() >= batchByteSize) {
            buffer.rewind();
        } else {
            buffer = ByteBuffer.allocateDirect(batchByteSize);
        }
        while (!pending.isEmpty()) {
            pending.remove().serializeOn(buffer);
        }
        buffer.flip();
        if (!writeBuffer()) {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
            return;
        }
        fsm.waiting();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBuffer() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.log(Level.INFO,
                        String.format("closing acknowledger %s ", fsm.getName()));
            } else {
                log.log(Level.WARNING,
                        String.format("Unable to write batch commit acknowledgement %s",
                                      fsm.getName()), e);
            }
            error();
            return false;
        }
        return !buffer.hasRemaining();
    }

    public void setFsmName(String fsmName) {
        fsm.setName(fsmName);
    }

    protected void enqueue(UUID channel, long sequenceNumber) {
        pending.add(new BatchIdentity(channel, sequenceNumber));
    }

    protected boolean hasPending() {
        return !pending.isEmpty();
    }

    /**
     * @param producer
     */
    public void setProducer(Node producer) {
        this.producer = producer;
    }
}
