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
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;
import com.salesforce.ouroboros.spindle.source.AbstractAppenderContext.AbstractAppenderState;

/**
 * The abstract appender of events. Instances of this class are responsible for
 * accepting inbound events from the socket channel and appending them to the
 * appropriate segment of the event channel to which they belong.
 * 
 * @author hhildebrand
 * 
 */
abstract public class AbstractAppender {

    private static final Logger             log      = Logger.getLogger(AbstractAppender.class.getCanonicalName());

    protected final BatchHeader             batchHeader;
    protected final Bundle                  bundle;
    protected ByteBuffer                    devNull;
    protected EventChannel                  eventChannel;
    protected final AbstractAppenderContext fsm      = new AbstractAppenderContext(
                                                                                   this);
    protected SocketChannelHandler          handler;
    protected boolean                       inError  = false;
    protected long                          offset   = -1L;
    protected long                          position = -1L;
    protected long                          remaining;
    protected Segment                       segment;

    public AbstractAppender(Bundle bundle) {
        super();
        this.bundle = bundle;
        batchHeader = createBatchHeader();
    }

    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
    }

    public void close() {
        handler.close();
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
            log.log(Level.SEVERE, "Exception during append", e);
            error();
            return false;
        }
        position += written;
        remaining -= written;
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Appending, position=%s, remaining=%s, written=%s",
                                    position, remaining, written));
        }
        if (remaining == 0) {
            try {
                segment.position(position);
            } catch (IOException e) {
                log.log(Level.SEVERE,
                        String.format("Cannot determine position in segment: %s",
                                      segment), e);
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
            log.warning(String.format("No existing event channel for: %s",
                                      batchHeader));
            fsm.drain();
            return;
        }
        AppendSegment logicalSegment = getLogicalSegment();
        segment = logicalSegment.segment;
        offset = position = logicalSegment.offset;
        remaining = batchHeader.getBatchByteLength();
        if (eventChannel.isDuplicate(batchHeader)) {
            log.warning(String.format("Duplicate event batch %s", batchHeader));
            fsm.drain();
            return;
        }
        if (append()) {
            fsm.appended();
        } else {
            handler.selectForRead();
        }
    }

    abstract protected void commit();

    abstract protected BatchHeader createBatchHeader();

    protected boolean devNull() {
        long read;
        try {
            read = handler.getChannel().read(devNull);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during append", e);
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
        devNull = ByteBuffer.allocate(batchHeader.getBatchByteLength());
        if (devNull()) {
            fsm.ready();
        }
    }

    protected void error() {
        if (segment != null) {
            try {
                segment.close();
            } catch (IOException e) {
                log.finest(String.format("Error closing segment %s", segment));
            }
        }
        segment = null;
        eventChannel = null;
        devNull = null;
    }

    abstract protected AppendSegment getLogicalSegment();

    protected boolean inError() {
        return inError;
    }

    protected void nextBatchHeader() {
        batchHeader.rewind();
        if (readBatchHeader()) {
            fsm.append();
        } else {
            handler.selectForRead();
        }
    }

    protected boolean readBatchHeader() {
        boolean read;
        try {
            read = batchHeader.read(handler.getChannel());
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during batch header read", e);
            error();
            return false;
        }
        if (read) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Batch header read, header=%s",
                                        batchHeader));
            }
            eventChannel = bundle.eventChannelFor(batchHeader.getChannel());
            return true;
        } else {
            return false;
        }
    }

    protected void selectForRead() {
        handler.selectForRead();
    }
}