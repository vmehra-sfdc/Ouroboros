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
package com.salesforce.ouroboros.spindle.replication;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.replication.DuplicatorContext.DuplicatorState;

/**
 * A duplicator of event streams. The duplicator provides outbound replication
 * of events sourced in the host process for a channel.
 * 
 * @author hhildebrand
 * 
 */
public final class Duplicator {

    static final Logger                     log     = Logger.getLogger(Duplicator.class.getCanonicalName());

    private EventEntry                      current;
    private final DuplicatorContext         fsm     = new DuplicatorContext(
                                                                            this);
    private SocketChannelHandler            handler;
    private boolean                         inError;
    private final BlockingQueue<EventEntry> pending = new LinkedBlockingQueue<EventEntry>();
    private long                            position;
    private int                             remaining;
    private final Node                      thisNode;

    public Duplicator(Node node) {
        fsm.setName(Integer.toString(node.processId));
        thisNode = node;
    }

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
    public void replicate(EventEntry event) {
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Replicating event %s on %s", event,
                                   thisNode));
        }
        pending.add(event);
        fsm.replicate();
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private boolean transferTo() throws IOException {
        int written = (int) current.segment.transferTo(position, remaining,
                                                       handler.getChannel());
        remaining -= written;
        position += written;
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Writing batch %s, position=%s, written=%s, to %s on %s",
                                    current.header, position, written,
                                    current.segment, thisNode));
        }
        return remaining == 0;
    }

    protected void close() {
        handler.close();
    }

    public void closing() {
        if (current != null) {
            try {
                current.segment.close();
            } catch (IOException e1) {
                log.log(Level.FINEST, String.format("Error closing segment %s",
                                                    current.segment), e1);
            }
        }
        current = null;
        for (EventEntry entry : pending) {
            entry.handler.selectForRead();
        }
        pending.clear();
    }

    protected boolean inError() {
        return inError;
    }

    protected void processBatch() {
        if (writeBatch()) {
            fsm.batchWritten();
        } else {
            if (inError) {
                fsm.close();
            } else {
                if (inError) {
                    fsm.close();
                } else {
                    handler.selectForWrite();
                }
            }
        }
    }

    protected void processHeader() {
        try {
            current = pending.take();
        } catch (InterruptedException e) {
            inError = true;
            fsm.close();
            return;
        }
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Processing %s on %s", current.header,
                                    thisNode));
        }
        current.handler.selectForRead();
        remaining = current.header.getBatchByteLength();
        position = current.header.getPosition();
        current.header.rewind();
        if (writeHeader()) {
            fsm.headerWritten();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
        }
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        try {
            if (transferTo()) {
                current.eventChannel.commit(current.header.getOffset());
                if (log.isLoggable(Level.FINER)) {
                    log.finer(String.format("Acknowledging replication of %s, on %s",
                                            current.header, thisNode));
                }
                current.acknowledger.acknowledge(current.header.getChannel(),
                                                 current.header.getTimestamp());
                current.segment.close();
                current = null;
                return true;
            }
        } catch (IOException e) {
            inError = true;
            log.log(Level.WARNING,
                    String.format("Unable to replicate payload for %s from: %s",
                                  current.header, current.segment), e);
        }
        return false;
    }

    protected boolean writeHeader() {
        try {
            if (current.header.write(handler.getChannel()) < 0) {
                inError = true;
                return false;
            }
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write batch header: %s",
                                  current.header), e);
            inError = true;
            return false;
        }
        boolean written = !current.header.hasRemaining();
        if (written) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("written header %s on %s",
                                        current.header, thisNode));
            }
        }
        return written;
    }

    protected boolean hasNext() {
        assert current == null : "Concurrent events are being processed";
        return pending.peek() != null;
    }

    protected void waitCheck() {
        if (hasNext()) {
            fsm.replicate();
        }
    }
}
