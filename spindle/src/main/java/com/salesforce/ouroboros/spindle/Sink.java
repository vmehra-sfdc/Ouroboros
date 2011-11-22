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

import static com.salesforce.ouroboros.spindle.Xerox.BUFFER_SIZE;
import static com.salesforce.ouroboros.spindle.Xerox.MAGIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.SinkContext.SinkState;

/**
 * The inbound half of the Xerox, which receives the bulk transfer from the
 * outbound Xerox machine.
 * 
 * @author hhildebrand
 * 
 */
public class Sink implements CommunicationsHandler {
    private final static Logger  log    = Logger.getLogger(Sink.class.getCanonicalName());

    private final ByteBuffer     buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final Bundle         bundle;
    private long                 bytesWritten;
    private EventChannel         channel;
    private Segment              current;
    private boolean              error;
    private final SinkContext    fsm    = new SinkContext(this);
    private SocketChannelHandler handler;
    private int                  segmentCount;
    private long                 segmentSize;

    public Sink(Bundle bundle) {
        this.bundle = bundle;
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.accept();
    }

    @Override
    public void closing() {
        if (current != null) {
            try {
                current.close();
            } catch (IOException e) {
                if (log.isLoggable(Level.FINEST)) {
                    log.log(Level.FINEST,
                            String.format("Error copying segment %s", current),
                            e);
                }
            }
        }
        channel = null;
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public SinkState getState() {
        return fsm.getState();
    }

    @Override
    public void readReady() {
        fsm.readReady();
    }

    @Override
    public void writeReady() {
        throw new UnsupportedOperationException();
    }

    protected void close() {
        handler.close();
    }

    protected boolean copy() {
        try {
            bytesWritten += current.transferFrom(handler.getChannel(),
                                                 bytesWritten, segmentSize
                                                               - bytesWritten);
        } catch (IOException e) {
            error = true;
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Error copying segment %s", current), e);
            }
            return false;
        }
        if (bytesWritten == segmentSize) {
            try {
                current.close();
            } catch (IOException e) {
                error = true;
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            String.format("Error closing segment %s", current),
                            e);
                }
                return false;
            }
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Sink segment %s copy finished on %s",
                                       current, bundle.getId()));
            }
            return true;
        }
        return false;
    }

    protected boolean inError() {
        return error;
    }

    protected boolean isLastSegment() {
        return segmentCount == 0;
    }

    protected void nextHandshake() {
        buffer.rewind();
        if (readHandshake()) {
            fsm.readHeader();
        } else {
            handler.selectForRead();
        }
    }

    protected void nextHeader() {
        segmentCount--;
        buffer.rewind();
        if (readHeader()) {
            fsm.copy();
        } else {
            handler.selectForRead();
        }
    }

    protected boolean readHandshake() {
        try {
            handler.getChannel().read(buffer);
        } catch (IOException e) {
            error = true;
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Error reading handshake on %s",
                                      bundle.getId()), e);
            }
            return false;
        }

        if (!buffer.hasRemaining()) {
            buffer.flip();
            long magic = buffer.getInt();
            if (MAGIC != magic) {
                error = true;
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid handshake magic value %s",
                                              magic));
                }
                return false;
            }
            segmentCount = buffer.getInt();
            UUID channelId = new UUID(buffer.getLong(), buffer.getLong());
            try {
                channel = bundle.createEventChannelFor(channelId);
            } catch (Throwable e) {
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            String.format("Unable to create channel % on %s",
                                          channelId, bundle.getId()), e);
                }
                return false;
            }
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Sink started for channel %s on %s",
                                       channelId, bundle.getId()));
            }
            return true;
        }
        return false;
    }

    protected boolean readHeader() {
        try {
            handler.getChannel().read(buffer);
        } catch (IOException e) {
            error = true;
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        String.format("Error reading header on %s",
                                      bundle.getId()), e);
            }
            return false;
        }

        if (!buffer.hasRemaining()) {
            buffer.flip();
            long magic = buffer.getLong();
            if (MAGIC != magic) {
                error = true;
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid handshake magic value %s",
                                              magic));
                }
                return false;
            }
            long offset = buffer.getLong();
            current = channel.segmentFor(offset);
            if (current == null) {
                error = true;
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("No segment for offset %s in channel %s",
                                              offset, channel));
                }
                return false;
            }
            segmentSize = buffer.getLong();
            bytesWritten = 0;
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Sink segment %s copy started on %s",
                                       current, bundle.getId()));
            }
            return true;
        }
        return false;
    }

    protected void selectForRead() {
        handler.selectForRead();
    }

    protected void startCopy() {
        if (copy()) {
            fsm.copyFinished();
        } else {
            handler.selectForRead();
        }
    }
}
