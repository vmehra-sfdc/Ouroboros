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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The concrete appender used to append events from producers.
 * 
 * @author hhildebrand
 * 
 */
public class Appender extends AbstractAppender {
    private static final Logger   log = Logger.getLogger(Appender.class.getCanonicalName());

    private final Bundle          bundle;
    private volatile EventChannel eventChannel;

    public Appender(Bundle bundle) {
        this.bundle = bundle;
    }

    @Override
    protected void commit() {
        eventChannel.append(header, offset, segment);
    }

    @Override
    protected void headerRead(SocketChannel channel) {
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
        segment = eventChannel.segmentFor(header);
        offset = position = eventChannel.nextOffset();
        remaining = header.size();
        if (eventChannel.isDuplicate(header)) {
            state = State.DEV_NULL;
            segment = null;
            devNull = ByteBuffer.allocate(header.size());
            devNull(channel);
        } else {
            writeHeader();
            append(channel);
        }
    }
}