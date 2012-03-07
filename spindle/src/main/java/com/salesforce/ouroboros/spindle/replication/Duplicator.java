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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.replication.DuplicatorContext.DuplicatorState;
import com.salesforce.ouroboros.util.Utils;

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
    private final Queue<EventEntry> pending = new LinkedList<EventEntry>();
    private long                    position;
    private int                     remaining;
    private final Node              thisNode;
    private AtomicBoolean           closed  = new AtomicBoolean();

    public Duplicator(Node node) {
        thisNode = node;
    }

    public void closing() {
        closed.set(true);
        if (current != null) {
            current.handler.selectForRead();
        }
        for (EventEntry entry : pending) {
            entry.handler.selectForRead();
        }
        pending.clear();
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
        if (closed.get()) {
            event.handler.selectForRead();
        } else {
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Replicating event %s on %s", event,
                                       fsm.getName()));
            }
            fsm.replicate(event);
        }
    }

    /**
     * @param format
     */
    public void setFsmName(String name) {
        fsm.setName(name);
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

    /**
     * @param event
     */
    protected void enqueue(EventEntry event) {
        pending.add(event);
    }

    protected boolean hasNext() {
        assert current == null : "Concurrent events are being processed";
        return pending.peek() != null;
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
        current = pending.remove();
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Processing %s on %s", current.header,
                                     fsm.getName()));
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
                                            current.header, fsm.getName()));
                }
                current.acknowledger.acknowledge(current.header.getChannel(),
                                                 current.header.getSequenceNumber());
                current = null;
                return true;
            }
        } catch (IOException e) {
            inError = true;
            if (Utils.isClose(e)) {
                log.log(Level.INFO,
                        String.format("closing duplicator: %s", fsm.getName()));
            } else {
                log.log(Level.WARNING,
                        String.format("Unable to replicate payload for %s from: %s",
                                      current.header, current.segment), e);
            }
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
            if (Utils.isClose(e)) {
                log.log(Level.INFO,
                        String.format("closing duplicator: %s", fsm.getName()));
            } else {
                log.log(Level.WARNING,
                        String.format("Unable to write batch header: %s",
                                      current.header), e);
            }
            inError = true;
            return false;
        }
        boolean written = !current.header.hasRemaining();
        if (written) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("written header %s on %s",
                                        current.header, fsm.getName()));
            }
        }
        return written;
    }
}
