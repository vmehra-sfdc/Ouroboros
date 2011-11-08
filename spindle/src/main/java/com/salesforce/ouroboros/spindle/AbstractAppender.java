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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;

/**
 * The abstract appender of events. Instances of this class are responsible for
 * accepting inbound events from the socket channel and appending them to the
 * appropriate segment of the event channel to which they belong.
 * 
 * @author hhildebrand
 * 
 */
abstract public class AbstractAppender {
    public enum State {
        APPEND, DEV_NULL, ERROR, INITIALIZED, READ_BATCH_HEADER, READY;
    }

    private static final Logger             log      = Logger.getLogger(AbstractAppender.class.getCanonicalName());

    protected final BatchHeader             batchHeader;
    protected final Bundle                  bundle;
    protected volatile ByteBuffer           devNull;
    protected volatile EventChannel         eventChannel;
    protected volatile SocketChannelHandler handler;
    protected volatile long                 offset   = -1L;
    protected volatile long                 position = -1L;
    protected volatile long                 remaining;
    protected volatile Segment              segment;
    protected volatile State                state    = State.INITIALIZED;

    public AbstractAppender(Bundle bundle) {
        super();
        this.bundle = bundle;
        batchHeader = createBatchHeader();
    }

    public State getState() {
        return state;
    }

    public void accept(SocketChannelHandler handler) {
        assert state == State.INITIALIZED;
        if (log.isLoggable(Level.FINER)) {
            log.finer("ACCEPT");
        }
        state = State.READY;
        this.handler = handler;
        this.handler.selectForRead();
    }

    public void readReady() {
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("READ, state=%s", state));
        }
        switch (state) {
            case READY: {
                initialRead();
                break;
            }
            case DEV_NULL: {
                devNull();
                break;
            }
            case READ_BATCH_HEADER: {
                readBatchHeader();
                break;
            }
            case APPEND: {
                append();
                break;
            }
            case ERROR: {
                log.info("Read encountered in ERROR state");
                return;
            }
            default: {
                log.severe(String.format("Invalid read state: %s", state));
                error();
                return;
            }
        }
        handler.selectForRead();
    }

    @Override
    public String toString() {
        return "Appender [state=" + state + ", segment=" + segment
               + ", remaining=" + remaining + ", position=" + position + "]";
    }

    private void drain() {
        state = State.DEV_NULL;
        segment = null;
        devNull = ByteBuffer.allocate(batchHeader.getBatchByteLength());
        devNull();
    }

    protected void append() {
        long written;
        try {
            written = segment.transferFrom(handler.getChannel(), position,
                                           remaining);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during append", e);
            error();
            return;
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
                return;
            }
            commit();
            segment = null;
            state = State.READY;
        }
    }

    abstract protected void commit();

    abstract protected BatchHeader createBatchHeader();

    protected void devNull() {
        long read;
        try {
            read = handler.getChannel().read(devNull);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during append", e);
            error();
            return;
        }
        position += read;
        remaining -= read;
        if (remaining == 0) {
            devNull = null;
            state = State.READY;
        }
    }

    protected void error() {
        state = State.ERROR;
        handler.close();
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

    protected void initialRead() {
        batchHeader.rewind();
        state = State.READ_BATCH_HEADER;
        readBatchHeader();
    }

    protected void readBatchHeader() {
        boolean read;
        try {
            read = batchHeader.read(handler.getChannel());
        } catch (IOException e) {
            log.log(Level.SEVERE, "Exception during batch header read", e);
            error();
            return;
        }
        if (read) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Batch header read, header=%s",
                                        batchHeader));
            }
            eventChannel = bundle.eventChannelFor(batchHeader.getChannel());
            if (eventChannel == null) {
                log.warning(String.format("No existing event channel for: %s",
                                          batchHeader));
                drain();
                return;
            }
            AppendSegment logicalSegment = getLogicalSegment();
            segment = logicalSegment.segment;
            offset = position = logicalSegment.offset;
            remaining = batchHeader.getBatchByteLength();
            if (eventChannel.isDuplicate(batchHeader)) {
                log.warning(String.format("Duplicate event batch %s",
                                          batchHeader));
                drain();
                return;
            }
            state = State.APPEND;
            append();
        }
    }
}