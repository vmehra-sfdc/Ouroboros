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
package com.salesforce.ouroboros.spindle.shuttle;

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

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.shuttle.ControllerContext.ControllerState;
import com.salesforce.ouroboros.util.Utils;

/**
 * Used to control the event push between the consumer and channel buffer.
 * 
 * @author hhildebrand
 * 
 */
public class Controller implements CommunicationsHandler {
    static final Logger               log            = LoggerFactory.getLogger(Controller.class);
    static final int                  MAX_BATCH_SIZE = 1000;
    static final int                  UUID_BYTE_SIZE = 2 * 8;

    private final ByteBuffer          buffer         = ByteBuffer.allocateDirect(MAX_BATCH_SIZE
                                                                                 * UUID_BYTE_SIZE);
    private final Thread              consumer;
    private final ArrayList<UUID>     drain          = new ArrayList<>(
                                                                       MAX_BATCH_SIZE);
    private final ControllerContext   fsm            = new ControllerContext(
                                                                             this);
    private SocketChannelHandler      handler;
    private boolean                   inError;
    private final BlockingQueue<UUID> pending        = new LinkedBlockingQueue<>();
    private final Semaphore           quantum        = new Semaphore(0);
    private final AtomicBoolean       run            = new AtomicBoolean(true);
    private final Node                self;

    public Controller(Node self) {
        consumer = new Thread(consumerAction());
        consumer.setName(String.format("Consumer thread for Acknowledger[?<%s]",
                                       self));
        this.self = self;
    }

    public void signal(UUID channel) {
        pending.add(channel);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.accept();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        if (log.isInfoEnabled()) {
            log.info(String.format("Closing signnaler %s", fsm.getName()));
        }
        fsm.close();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#connect(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public ControllerState getState() {
        return fsm.getState();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#readReady()
     */
    @Override
    public void readReady() {
        fsm.readReady();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    private void error() {
        inError = true;
    }

    protected void close() {
        handler.close();
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
                            UUID id = pending.poll(1, TimeUnit.SECONDS);
                            if (id != null) {
                                drain.add(id);
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

    protected void establish() {
        buffer.flip();
        Node n = new Node(buffer);
        fsm.setName(String.format("%s>%s", self, n));
        consumer.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.warn(String.format("Uncaught exception on %s", t), e);
            }
        });
        buffer.limit(buffer.capacity());
        buffer.rewind();
        consumer.setDaemon(true);
        consumer.start();
    }

    protected void handshake() {
        buffer.limit(Node.BYTE_LENGTH);
        if (!readHandshake()) {
            if (inError()) {
                fsm.close();
            } else {
                selectForRead();
            }
        } else {
            fsm.established();
        }
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
        for (UUID id : drain) {
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
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

    protected void nextQuantum() {
        quantum.release();
    }

    /**
     * @return true if the handshake was completely read, false otherwise
     */
    protected boolean readHandshake() {
        try {
            if (handler.getChannel().read(buffer) < 0) {
                error();
                return false;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception reading handshake on %s",
                                           self), e);
                }
            }
            error();
            return false;
        }
        if (buffer.hasRemaining()) {
            return false;
        } else {
            establish();
            return true;
        }
    }

    protected void selectForRead() {
        handler.selectForRead();
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
}
