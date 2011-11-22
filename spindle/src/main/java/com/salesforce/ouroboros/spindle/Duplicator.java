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
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.DuplicatorContext.DuplicatorState;
import com.salesforce.ouroboros.util.lockfree.LockFreeQueue;

/**
 * A duplicator of event streams. The duplicator provides outbound replication
 * of events sourced in the host process for a channel.
 * 
 * @author hhildebrand
 * 
 */
public final class Duplicator {

    static final Logger             log     = Logger.getLogger(Duplicator.class.getCanonicalName());

    private EventEntry              current;
    private final DuplicatorContext fsm     = new DuplicatorContext(this);
    private SocketChannelHandler    handler;
    private boolean                 inError;
    private long                    position;
    private int                     remaining;
    final Queue<EventEntry>         pending = new LockFreeQueue<EventEntry>();

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
    }

    /**
     * @return the state of the outbound replicator
     */
    public DuplicatorState getState() {
        return fsm.getState();
    }

    /**
     * Replicate the event to the mirror
     */
    public void replicate(ReplicatedBatchHeader header,
                          EventChannel eventChannel, Segment segment,
                          Acknowledger acknowledger) {
        pending.add(new EventEntry(header, eventChannel, segment, acknowledger));
        if (!fsm.isInTransition()) {
            fsm.replicate();
        }
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private boolean transferTo() throws IOException {
        long p = position;
        int written = (int) current.segment.transferTo(p, remaining,
                                                       handler.getChannel());
        remaining = remaining - written;
        position = p + written;
        if (remaining == 0) {
            return true;
        }
        return false;
    }

    protected void close() {
        if (current != null) {
            try {
                current.segment.close();
            } catch (IOException e1) {
                log.log(Level.FINEST, String.format("Error closing segment %s",
                                                    current.segment), e1);
            }
        }
        current = null;
        pending.clear();
        handler.close();
    }

    protected boolean inError() {
        return inError;
    }

    protected void processBatch() {
        if (writeBatch()) {
            fsm.batchWritten();
        } else {
            handler.selectForWrite();
        }
    }

    protected void processHeader() {
        current = pending.poll();
        if (current == null) {
            fsm.pendingEmpty();
        }
        remaining = current.header.getBatchByteLength();
        position = current.header.getOffset();
        current.header.rewind();
        if (writeHeader()) {
            fsm.headerWritten();
        } else {
            handler.selectForWrite();
        }
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        try {
            if (transferTo()) {
                current.eventChannel.commit(current.header.getOffset());
                current.acknowledger.acknowledge(current.header.getChannel(),
                                                 current.header.getTimestamp());
                return true;
            }
        } catch (IOException e) {
            inError = true;
            log.log(Level.WARNING,
                    String.format("Unable to replicate payload for event: %s from: %s",
                                  current.header.getOffset(), current.segment),
                    e);
        }
        return false;
    }

    protected boolean writeHeader() {
        boolean written = false;
        try {
            written = current.header.write(handler.getChannel());
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write batch header: %s",
                                  current.header), e);
            inError = true;
        }
        return written;
    }
}
