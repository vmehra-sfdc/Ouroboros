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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * @author hhildebrand
 * 
 */
public class Shuttle implements CommunicationsHandler {
    private SocketChannelHandler             handler;
    private final WeftHeader                 header        = new WeftHeader();
    private final ConcurrentMap<UUID, Flyer> subscriptions = new ConcurrentHashMap<>();

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#connect(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#readReady()
     */
    @Override
    public void readReady() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        // TODO Auto-generated method stub

    }

    /**
     * 
     */
    protected void close() {
        handler.close();
    }

    /**
     * 
     */
    protected void established() {
        // TODO Auto-generated method stub

    }

    /**
     * 
     */
    protected void handshake() {
        // TODO Auto-generated method stub

    }

    /**
     * @return
     */
    protected boolean inError() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * @return
     */
    protected boolean readHandshake() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * 
     */
    protected void selectForRead() {
        // TODO Auto-generated method stub

    }
}
