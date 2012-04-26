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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.replication.DuplicatorContext.DuplicatorState;
import com.salesforce.ouroboros.spindle.source.Acknowledger;
import com.salesforce.ouroboros.util.Utils;

/**
 * A duplicator of event streams. The duplicator provides outbound replication
 * of events sourced in the host process for a channel.
 * 
 * @author hhildebrand
 * 
 */
public final class Duplicator {

    static final Logger             log     = LoggerFactory.getLogger(Duplicator.class.getCanonicalName());

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
            current.getHandler().selectForRead();
        }
        for (EventEntry entry : pending) {
            entry.getHandler().selectForRead();
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
     * @param format
     */
    public void setFsmName(String name) {
        fsm.setName(name);
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private boolean transferTo() throws IOException {
        int written = (int) current.getSegment().transferTo(position,
                                                            remaining,
                                                            handler.getChannel());
        remaining -= written;
        position += written;
        if (log.isTraceEnabled()) {
            log.trace(String.format("Writing batch %s, position=%s, written=%s, to %s on %s",
                                    current.getHeader(), position, written,
                                    current.getSegment(), thisNode));
        }
        return remaining == 0;
    }

    protected void close() {
        handler.close();
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
        if (log.isTraceEnabled()) {
            log.trace(String.format("Processing %s on %s", current.getHeader(),
                                    fsm.getName()));
        }
        current.getHandler().selectForRead();
        remaining = current.getHeader().getBatchByteLength();
        position = current.getHeader().getPosition();
        current.getHeader().rewind();
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
                current.getEventChannel().commit(current.getHeader().getOffset());
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Acknowledging replication of %s, on %s",
                                            current.getHeader(), fsm.getName()));
                }
                current.getAcknowledger().acknowledge(current.getHeader().getChannel(),
                                                      current.getHeader().getSequenceNumber());
                current = null;
                return true;
            }
        } catch (IOException e) {
            inError = true;
            if (Utils.isClose(e)) {
                log.info(String.format("closing duplicator: %s", fsm.getName()));
            } else {
                log.warn(String.format("Unable to replicate payload for %s from: %s",
                                       current.getHeader(),
                                       current.getSegment()), e);
            }
        }
        return false;
    }

    protected boolean writeHeader() {
        try {
            if (current.getHeader().write(handler.getChannel()) < 0) {
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing duplicator: %s", fsm.getName()));
            } else {
                log.warn(String.format("Unable to write batch header: %s",
                                       current.getHeader()), e);
            }
            inError = true;
            return false;
        }
        boolean written = !current.getHeader().hasRemaining();
        if (written) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("written header %s on %s",
                                        current.getHeader(), fsm.getName()));
            }
        }
        return written;
    }

    /**
     * Replicate the event to the mirror
     * 
     * @param header
     * @param eventChannel
     * @param segment
     * @param acknowledger
     * @param sourceHandler
     */
    public void replicate(ReplicatedBatchHeader header,
                          EventChannel eventChannel, Segment segment,
                          Acknowledger acknowledger,
                          SocketChannelHandler sourceHandler) {
        if (closed.get()) {
            sourceHandler.selectForRead();
        } else {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Replicating batch %s on %s", header,
                                        fsm.getName()));
            }
            fsm.replicate(header, eventChannel, segment, acknowledger,
                          sourceHandler);
        }
    }

    /**
     * @param header
     * @param eventChannel
     * @param segment
     * @param acknowledger
     * @param sourceHandler
     */
    protected void enqueue(ReplicatedBatchHeader header,
                           EventChannel eventChannel, Segment segment,
                           Acknowledger acknowledger,
                           SocketChannelHandler sourceHandler) {
        EventEntry entry = new EventEntry();
        entry.set(header, eventChannel, segment, acknowledger, sourceHandler);
        pending.add(entry);
    }
}
