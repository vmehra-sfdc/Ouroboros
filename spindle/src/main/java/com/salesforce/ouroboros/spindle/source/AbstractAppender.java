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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.source.AbstractAppenderContext.AbstractAppenderState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The abstract appender of events. Instances of this class are responsible for
 * accepting inbound events from the socket channel and appending them to the
 * appropriate segment of the event channel to which they belong.
 * 
 * @author hhildebrand
 * 
 */
abstract public class AbstractAppender {
    protected final BatchHeader             batchHeader;
    protected final Bundle                  bundle;
    protected ByteBuffer                    devNull;
    protected EventChannel                  eventChannel;
    protected final AbstractAppenderContext fsm      = new AbstractAppenderContext(
                                                                                   this);
    protected SocketChannelHandler          handler;
    protected boolean                       inError  = false;
    protected long                          offset   = -1L;
    protected int                           position = -1;
    protected long                          remaining;
    protected Segment                       segment;

    public AbstractAppender(Bundle bundle) {
        fsm.setName(Integer.toString(bundle.getId().processId));
        this.bundle = bundle;
        batchHeader = createBatchHeader();
    }

    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
    }

    public void close() {
        handler.close();
    }

    public void closing() {
    }

    public AbstractAppenderState getState() {
        return fsm.getState();
    }

    public void readReady() {
        fsm.readReady();
    }

    protected boolean append() {
        long written;
        try {
            written = segment.transferFrom(handler.getChannel(), position,
                                           remaining);
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                getLogger().log(Level.INFO,
                                String.format("closing appender %s ",
                                              fsm.getName()));
            } else {
                getLogger().log(Level.SEVERE,
                                String.format("Exception during append on %s",
                                              fsm.getName()), e);
            }
            error();
            return false;
        }
        position += written;
        remaining -= written;
        if (getLogger().isLoggable(Level.FINER)) {
            getLogger().finer(String.format("Appending, offset=%s, position=%s, remaining=%s, written=%s on %s",
                                            offset, position, remaining,
                                            written, fsm.getName()));
        }
        if (remaining == 0) {
            try {
                segment.position(position);
            } catch (IOException e) {
                getLogger().log(Level.SEVERE,
                                String.format("Cannot determine position on channel %s segment: %s on %s",
                                              eventChannel, segment,
                                              fsm.getName()), e);
                error();
                return false;
            }
            commit();
            segment = null;
            return true;
        }
        return false;
    }

    protected boolean batchHeaderWritten() {
        return !batchHeader.hasRemaining();
    }

    protected void beginAppend() {
        if (eventChannel == null) {
            getLogger().warning(String.format("No existing event channel for: %s on %s",
                                              batchHeader, fsm.getName()));
            fsm.drain();
            return;
        }
        AppendSegment logicalSegment;
        try {
            logicalSegment = getLogicalSegment();
        } catch (IOException e) {
            getLogger().log(Level.WARNING,
                            String.format("Cannot retrieve segment, shutting down appender %s",
                                          fsm.getName()), e);
            error();
            return;
        }
        segment = logicalSegment.segment;
        offset = logicalSegment.offset;
        position = logicalSegment.position;
        markPosition();
        remaining = batchHeader.getBatchByteLength();
        if (eventChannel.isDuplicate(batchHeader)) {
            getLogger().warning(String.format("Duplicate event batch %s received on %s",
                                              batchHeader, fsm.getName()));
            fsm.drain();
            return;
        }
        if (getLogger().isLoggable(Level.FINER)) {
            getLogger().finer(String.format("Beginning append of %s, offset=%s, position=%s, remaining=%s on %s",
                                            segment, offset, position,
                                            remaining, fsm.getName()));
        }
        if (append()) {
            fsm.appended();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForRead();
            }
        }
    }

    abstract protected void commit();

    abstract protected BatchHeader createBatchHeader();

    protected boolean devNull() {
        long read;
        try {
            if ((read = handler.getChannel().read(devNull)) < 0) {
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().fine("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                getLogger().log(Level.INFO,
                                String.format("closing appender %s ",
                                              fsm.getName()));
            } else {
                getLogger().log(Level.SEVERE,
                                String.format("Exception during append on %s",
                                              fsm.getName()), e);
            }
            error();
            return false;
        }
        position += read;
        remaining -= read;
        if (remaining == 0) {
            devNull = null;
            return true;
        }
        return false;
    }

    protected void drain() {
        segment = null;
        devNull = ByteBuffer.allocate(32 * 1024);
        if (devNull()) {
            fsm.ready();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForRead();
            }
        }
    }

    protected void error() {
        inError = true;
        segment = null;
        eventChannel = null;
        devNull = null;
    }

    abstract protected Logger getLogger();

    abstract protected AppendSegment getLogicalSegment() throws IOException;

    protected boolean inError() {
        return inError;
    }

    protected void markPosition() {
        // default is to do nothing
    }

    protected void nextBatchHeader() {
        batchHeader.rewind();
        if (readBatchHeader()) {
            fsm.append();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForRead();
            }
        }
    }

    protected boolean readBatchHeader() {
        try {
            if (batchHeader.read(handler.getChannel()) < 0) {
                error();
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                getLogger().log(Level.INFO,
                                String.format("closing appender %s ",
                                              fsm.getName()));
            } else {
                getLogger().log(Level.SEVERE,
                                String.format("Exception during batch header read on %s",
                                              fsm.getName()), e);
            }
            error();
            return false;
        }
        if (!batchHeader.hasRemaining()) {
            if (getLogger().isLoggable(Level.FINER)) {
                getLogger().finer(String.format("Batch header read, header=%s on %s",
                                                batchHeader, fsm.getName()));
            }
            if (batchHeader.getMagic() != BatchHeader.MAGIC) {
                getLogger().severe(String.format("Received invalid magic %s in header %s on %s",
                                                 batchHeader.getMagic(),
                                                 batchHeader, fsm.getName()));
                error();
                return false;
            }
            eventChannel = bundle.eventChannelFor(batchHeader.getChannel());
            return true;
        } else {
            return false;
        }
    }

    protected void ready() {
        // default is to do nothing
    }

    protected void selectForRead() {
        handler.selectForRead();
    }
}