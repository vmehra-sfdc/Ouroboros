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
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
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
    static final Logger                        log            = LoggerFactory.getLogger(Acknowledger.class.getCanonicalName());
    static final int                           MAX_BATCH_SIZE = 1000;

    private final ByteBuffer                   buffer         = ByteBuffer.allocateDirect(MAX_BATCH_SIZE
                                                                                          * BatchIdentity.BYTE_SIZE);
    private final Bundle                       bundle;
    private final AcknowledgerContext          fsm            = new AcknowledgerContext(
                                                                                        this);
    private SocketChannelHandler               handler;
    private boolean                            inError;
    private final BlockingQueue<BatchIdentity> pending        = new LinkedBlockingQueue<>();
    private Node                               producer;
    private final ArrayList<BatchIdentity>     drain          = new ArrayList<>(
                                                                                MAX_BATCH_SIZE);
    private final AtomicBoolean                run            = new AtomicBoolean(
                                                                                  true);
    private final Thread                       consumer;
    private final Semaphore                    quantum        = new Semaphore(0);

    public Acknowledger(Bundle bundle) {
        this.bundle = bundle;
        consumer = new Thread(consumerAction());
        consumer.setName(String.format("Consumer thread for Acknowledger[?<%s]",
                                       bundle.getId()));
    }

    public void close() {
        handler.close();
    }

    public void closing() {
        run.set(false);
        quantum.release(10);
        pending.clear();
        bundle.closeAcknowledger(producer);
    }

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        consumer.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn(String.format("Uncaught exception on %s", t), e);
            }
        });
        consumer.setDaemon(true);
        consumer.start();
        fsm.connect();
    }

    public AcknowledgerState getState() {
        return fsm.getState();
    }

    public void setFsmName(String fsmName) {
        fsm.setName(fsmName);
        consumer.setName(String.format("Consumer thread for Acknowledger[%s]",
                                       fsmName));
    }

    /**
     * @param producer
     */
    public void setProducer(Node producer) {
        this.producer = producer;
    }

    public void writeReady() {
        fsm.writeReady();
    }

    private void error() {
        inError = true;
    }

    public void acknowledge(UUID channel, long sequenceNumber) {
        pending.add(new BatchIdentity(channel, sequenceNumber));
    }

    protected boolean hasNext() {
        return !pending.isEmpty();
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextBatch() {
        buffer.clear();
        pending.drainTo(drain, MAX_BATCH_SIZE - 1);
        for (BatchIdentity bid : drain) {
            bid.serializeOn(buffer);
        }
        drain.clear();
        buffer.flip();
        if (writeBatch()) {
            fsm.payloadWritten();
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

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeBatch() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing channel");
                }
                inError = true;
                return false;
            } else if (buffer.hasRemaining()) {
                if (handler.getChannel().write(buffer) < 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("Closing channel");
                    }
                    inError = true;
                    return false;
                }
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing acknowledger %s ",
                                       fsm.getName()));
            } else {
                log.warn(String.format("Unable to write batch commit acknowledgement %s",
                                       fsm.getName()), e);
            }
            error();
            return false;
        }
        return !buffer.hasRemaining();
    }

    protected Runnable consumerAction() {
        return new Runnable() {
            @Override
            public void run() {
                while (run.get()) {
                    try {
                        quantum.acquire();
                        do {
                            if (!run.get()) {
                                return;
                            }
                            BatchIdentity bid = pending.poll(1,
                                                             TimeUnit.SECONDS);
                            if (bid != null) {
                                drain.add(bid);
                            }
                        } while (drain.isEmpty());
                    } catch (InterruptedException e) {
                        return;
                    }
                    fsm.writeBatch();
                }
            }
        };
    }

    protected void nextQuantum() {
        quantum.release();
    }
}
