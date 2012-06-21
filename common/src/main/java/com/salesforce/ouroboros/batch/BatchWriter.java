/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.batch;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.util.Utils;

/**
 * @author hhildebrand
 * 
 *         A generic decoupled batching writer of fixed event types
 */
public class BatchWriter<EventType extends BufferSerializable> {
    private static final Logger            log     = LoggerFactory.getLogger(BatchWriter.class);

    private final int                      batchSize;
    private final ByteBuffer               buffer;
    private final Thread                   consumer;
    private final List<EventType>          drain;
    private final int                      eventByteSize;
    protected final BatchWriterContext     fsm     = new BatchWriterContext(
                                                                            this);
    private SocketChannelHandler           handler;
    private boolean                        inError;
    private final BlockingDeque<EventType> pending;
    private final Semaphore                quantum = new Semaphore(0);
    private final String                   threadNameFormat;
    private final AtomicBoolean            run     = new AtomicBoolean(true);

    public BatchWriter(int ebs, int bs, String nameFormat) {
        eventByteSize = ebs;
        batchSize = bs;
        buffer = ByteBuffer.allocateDirect(eventByteSize * batchSize);
        consumer = new Thread(consumerAction());
        consumer.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn(String.format("Uncaught exception on %s", t), e);
            }
        });
        consumer.setDaemon(true);
        drain = new ArrayList<EventType>();
        pending = new LinkedBlockingDeque<EventType>(batchSize);
        this.threadNameFormat = "Consumer thread for " + nameFormat;
    }

    public void close() {
        handler.close();
    }

    public void closing() {
        run.set(false);
        quantum.release(10);
        pending.clear();
    };

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        consumer.start();
        fsm.connect();
    }

    public boolean inError() {
        return inError;
    };

    public void nextBatch() {
        while (!drain.isEmpty()) {
            buffer.clear();
            for (EventType bid : drain) {
                bid.serializeOn(buffer);
            }
            drain.clear();
            buffer.flip();
            if (!writeBatch()) {
                if (inError) {
                    fsm.close();
                } else {
                    buffer.compact();
                    // See if we can pack more in zee buffer
                    int space = buffer.remaining() / eventByteSize;
                    if (space > 0) {
                        pending.drainTo(drain, space);
                        if (!drain.isEmpty()) {
                            for (EventType bid : drain) {
                                bid.serializeOn(buffer);
                            }
                        }
                    }
                    buffer.flip();
                    handler.selectForWrite();
                }
                return;
            }
            pending.drainTo(drain);
        }
        fsm.payloadWritten();
    };

    public void nextQuantum() {
        quantum.release();
    };

    public void selectForWrite() {
        handler.selectForWrite();
    };

    public void send(EventType event) {
        pending.add(event);
    };

    public void setFsmName(String fsmName) {
        fsm.setName(fsmName);
        consumer.setName(String.format(threadNameFormat, fsmName));
    };

    public boolean writeBatch() {
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
                log.info(String.format("closing BatchWriter %s ", fsm.getName()));
            } else {
                log.warn(String.format("Unable to write batch commit %s",
                                       fsm.getName()), e);
            }
            inError = true;
            return false;
        }
        return !buffer.hasRemaining();
    }

    public void writeReady() {
        fsm.writeReady();
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
                            EventType event = pending.poll(1, TimeUnit.SECONDS);
                            if (event != null) {
                                drain.add(event);
                                pending.drainTo(drain, batchSize - 1);
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
}
