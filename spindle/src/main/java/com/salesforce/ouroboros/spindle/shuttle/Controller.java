package com.salesforce.ouroboros.spindle.shuttle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.EventSpan;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.util.Utils;

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

/**
 * @author hhildebrand
 * 
 */
public class Controller implements CommunicationsHandler {
    private static final Logger          log         = LoggerFactory.getLogger(Controller.class);

    private final Bundle                 bundle;
    private boolean                      error;
    private final AtomicBoolean          established = new AtomicBoolean();
    private final ControllerContext      fsm         = new ControllerContext(
                                                                             this);
    private SocketChannelHandler         handler;
    private ByteBuffer                   handshake   = ByteBuffer.allocateDirect(Node.BYTE_LENGTH);
    private final PushNotification       pushNotification;
    private final SubscriptionManagement subscriptionManagement;

    public Controller(Bundle b) {
        bundle = b;
        pushNotification = new PushNotification(EventSpan.BYTE_SIZE, 1000,
                                                "Push Notification[%s]", bundle);
        pushNotification.setFsmName(String.format("%s>?", bundle.getId()));
        subscriptionManagement = new SubscriptionManagement(bundle.getId(),
                                                            pushNotification);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.accept();
    }

    public void close() {
        handler.close();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        pushNotification.closing();
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
        if (established.get()) {
            subscriptionManagement.readReady();
        } else {
            fsm.readReady();
        }
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        if (established.get()) {
            pushNotification.writeReady();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected void established() {
        handshake.flip();
        Node n = new Node(handshake);
        String fsmName = String.format("%s>%s", bundle.getId(), n);
        fsm.setName(fsmName);
        pushNotification.setFsmName(fsmName);
        established.set(true);
        pushNotification.connect(handler);
    }

    protected void handshake() {
        if (!readHandshake()) {
            if (error) {
                fsm.close();
            } else {
                handler.selectForRead();
            }
        } else {
            fsm.established();
        }
    }

    protected boolean inError() {
        return error;
    }

    /**
     * @return true if the handshake was completely read, false otherwise
     */
    protected boolean readHandshake() {
        try {
            if (handler.getChannel().read(handshake) < 0) {
                error = true;
                return false;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception reading handshake on %s",
                                           bundle.getId()), e);
                }
            }
            error = true;
            return false;
        }
        if (handshake.hasRemaining()) {
            return false;
        } else {
            return true;
        }
    }

    protected void selectForRead() {
        handler.selectForRead();
    }
}
