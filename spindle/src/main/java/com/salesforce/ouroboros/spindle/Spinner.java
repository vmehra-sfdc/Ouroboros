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

/**
 * The inbound event sink for event channels. The spinner has an appender that
 * appends inbound events to the correct segment of the event channel to which
 * the event belongs. When the event is fully appended, the event is commited by
 * queuing the event for replication.
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {
    static final Logger    log = Logger.getLogger(Spinner.class.getCanonicalName());

    private final Appender appender;

    public Spinner(Bundle bundle) {
        appender = new Appender(bundle);
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    public AbstractAppender.State getState() {
        return appender.getState();
    }

    @Override
    public void handleAccept(SocketChannel channel,
                             SocketChannelHandler<?> handler) {
        appender.handleAccept(channel, handler);
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler<?> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleRead(SocketChannel channel) {
        appender.handleRead(channel);
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        throw new UnsupportedOperationException();
    }
}
