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

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;

/**
 * The concrete appender used to append events from producers.
 * 
 * @author hhildebrand
 * 
 */
public class Appender extends AbstractAppender implements CommunicationsHandler {
    private static final Logger log = Logger.getLogger(Appender.class.getCanonicalName());

    public Appender(Bundle bundle) {
        super(bundle);
    }

    @Override
    public void closing(SocketChannel channel) {
        log.fine(String.format("Closing appender for %s", bundle));
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler<? extends CommunicationsHandler> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void commit() {
        eventChannel.append(new ReplicatedBatchHeader(batchHeader, offset),
                            segment);
    }

    @Override
    protected BatchHeader createBatchHeader() {
        return new BatchHeader();
    }

    @Override
    protected AppendSegment getLogicalSegment() {
        return eventChannel.segmentFor(batchHeader);
    }
}