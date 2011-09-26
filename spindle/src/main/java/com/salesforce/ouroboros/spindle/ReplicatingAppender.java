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
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The appender for receiving duplicated events from the primary.
 * 
 * @author hhildebrand
 * 
 */
public class ReplicatingAppender extends AbstractAppender {
    private static final Logger log          = Logger.getLogger(ReplicatingAppender.class.getCanonicalName());

    private final ByteBuffer    offsetBuffer = ByteBuffer.allocate(8);

    public ReplicatingAppender(final Bundle bundle) {
        super(bundle);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.AbstractAppender#commit()
     */
    @Override
    protected void commit() {
        eventChannel.append(header, offset);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.AbstractAppender#headerRead(java.nio.channels.SocketChannel)
     */
    @Override
    protected void headerRead(SocketChannel channel) {
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Header read, header=%s", header));
        }
        eventChannel = bundle.eventChannelFor(header);
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Header read, header=%s", header));
        }
        if (eventChannel == null) {
            log.warning(String.format("No existing event channel for: %s",
                                      header));
            state = State.DEV_NULL;
            segment = null;
            devNull = ByteBuffer.allocate(header.size());
            devNull(channel);
            return;
        }
        segment = eventChannel.segmentFor(offset);
        remaining = header.size();
        if (!eventChannel.isNextAppend(offset)) {
            state = State.DEV_NULL;
            segment = null;
            devNull = ByteBuffer.allocate(header.size());
            devNull(channel);
        } else {
            writeHeader();
            append(channel);
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Appender#initialRead(java.nio.channels.SocketChannel)
     */
    @Override
    protected void initialRead(SocketChannel channel) {
        state = State.READ_OFFSET;
        offsetBuffer.clear();
        readOffset(channel);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Appender#readOffset(java.nio.channels.SocketChannel)
     */
    @Override
    protected void readOffset(SocketChannel channel) {
        try {
            channel.read(offsetBuffer);
        } catch (IOException e) {
            log.log(Level.WARNING, "Exception during offset read", e);
            error();
            return;
        }
        if (!offsetBuffer.hasRemaining()) {
            offsetBuffer.flip();
            offset = position = offsetBuffer.getLong();
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Offset read, offset=%s", offset));
            }
            state = State.READ_HEADER;
            readHeader(channel);
        }
    }
}