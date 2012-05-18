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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static final Logger                     log      = LoggerFactory.getLogger(Duplicator.class.getCanonicalName());

    private AtomicBoolean                   closed   = new AtomicBoolean();
    private final Thread                    consumer;
    private EventEntry                      current;
    private final DuplicatorContext         fsm      = new DuplicatorContext(
                                                                             this);
    private SocketChannelHandler            handler;
    private boolean                         inError;
    private final Queue<EventEntry>         inFlight = new LinkedList<EventEntry>();
    private final int                       maxBatchedSize;
    private final BlockingDeque<EventEntry> pending  = new LinkedBlockingDeque<EventEntry>();
    private long                            position;
    private final Semaphore                 quantum  = new Semaphore(1);
    private int                             remaining;
    private final Node                      thisNode;

    public Duplicator(Node node, int maxBatchedSize) {
        thisNode = node;
        this.maxBatchedSize = maxBatchedSize;
        consumer = new Thread(
                              consumerAction(),
                              String.format("Consumer thread for Duplicator[%s>?]",
                                            thisNode));
        consumer.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn(String.format("Uncaught exception on %s", t), e);
            }
        });
        consumer.setDaemon(true);
        consumer.start();
    }

    public void closing() {
        closed.set(true);
        if (current != null) {
            current.free();
        }
        for (EventEntry entry : pending) {
            entry.free();
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
            event.free();
        } else {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Replicating event %s on %s", event,
                                        fsm.getName()));
            }
            pending.add(event);
        }
    }

    /**
     * @param format
     */
    public void setFsmName(String name) {
        fsm.setName(name);
        consumer.setName(String.format("Consumer thread for Duplicator[%s]",
                                       name));
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private boolean next() {
        current = inFlight.poll();

        if (current == null) {
            return false;
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Processing %s:%s on %s",
                                    current.getHeader().getChannel(),
                                    current.getHeader().getSequenceNumber(),
                                    fsm.getName()));
        }
        remaining = current.getHeader().getBatchByteLength();
        position = current.getHeader().getPosition();
        current.getHeader().rewind();
        return true;
    }

    private boolean transferTo() throws IOException {
        int written = (int) current.getSegment().transferTo(position,
                                                            remaining,
                                                            handler.getChannel());
        remaining -= written;
        position += written;
        if (log.isTraceEnabled()) {
            log.trace(String.format("Writing batch %s:%s, position=%s, written=%s, to %s on %s",
                                    current.getHeader().getChannel(),
                                    current.getHeader().getSequenceNumber(),
                                    position, written, current.getSegment(),
                                    thisNode));
        }
        return remaining == 0;
    }

    protected void close() {
        handler.close();
    }

    protected Runnable consumerAction() {
        return new Runnable() {

            @Override
            public void run() {
                while (!closed.get()) {
                    EventEntry first = null;
                    try {
                        quantum.acquire();
                        do {
                            first = pending.poll(4, TimeUnit.SECONDS);
                            if (closed.get()) {
                                return;
                            }
                        } while (first == null);
                    } catch (InterruptedException e) {
                        return;
                    }
                    inFlight.add(first);
                    pending.drainTo(inFlight);
                    if (log.isTraceEnabled()) {
                        log.trace(String.format("pushing %s batches",
                                                inFlight.size()));
                    }
                    fsm.replicate();
                }
            }
        };
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextQuantum() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Releasing next quantum, %s waiting",
                                    pending.size()));
        }
        quantum.release();
    }

    protected void batchReplicate() {
        int count = 0;
        while (count++ < maxBatchedSize && next()) {
            if (!replicate()) {
                if (inError) {
                    fsm.close();
                }
                return;
            }
        }
        if (inError) {
            fsm.close();
        } else {
            fsm.quantumProcessed();
        }
    }

    protected boolean replicate() {
        if (writeHeader()) {
            if (writeBatch()) {
                return true;
            } else {
                if (inError) {
                    fsm.close();
                }
                return false;
            }
        } else {
            if (inError) {
                fsm.close();
            }
            return false;
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
                    log.trace(String.format("Acknowledging replication of %s:%s, on %s",
                                            current.getHeader().getChannel(),
                                            current.getHeader().getSequenceNumber(),
                                            fsm.getName()));
                }
                current.getAcknowledger().acknowledge(current.getHeader().getChannel(),
                                                      current.getHeader().getSequenceNumber());
                current.free();
                current = null;
                return true;
            }
        } catch (IOException e) {
            inError = true;
            if (Utils.isClose(e)) {
                log.info(String.format("closing duplicator: %s", fsm.getName()));
            } else {
                log.warn(String.format("Unable to replicate payload for %s:%s from: %s",
                                       current.getHeader().getChannel(),
                                       current.getHeader().getSequenceNumber(),
                                       current.getSegment()), e);
            }
        }
        fsm.writeBatch();
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
                log.warn(String.format("Unable to write batch header: %s:%s",
                                       current.getHeader().getChannel(),
                                       current.getHeader().getSequenceNumber()),
                         e);
            }
            inError = true;
            return false;
        }
        if (!current.getHeader().hasRemaining()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("written header %s:%s on %s",
                                        current.getHeader().getChannel(),
                                        current.getHeader().getSequenceNumber(),
                                        fsm.getName()));
            }
            return true;
        } else {
            fsm.writeHeader();
            return false;
        }
    }
}
