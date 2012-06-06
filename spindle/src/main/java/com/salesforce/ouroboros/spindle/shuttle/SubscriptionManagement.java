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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.SubscriptionEvent;
import com.salesforce.ouroboros.spindle.shuttle.SubscriptionManagementContext.SubscriptionManagementState;
import com.salesforce.ouroboros.util.Utils;

/**
 * @author hhildebrand
 * 
 */
public class SubscriptionManagement {
    private static final Logger                 log            = LoggerFactory.getLogger(SubscriptionManagement.class);
    private static final int                    MAX_BATCH_SIZE = 1000;

    private boolean                             error;
    private final ByteBuffer                    eventBuffer    = ByteBuffer.allocate(SubscriptionEvent.BYTE_SIZE
                                                                                     * MAX_BATCH_SIZE);
    private final SubscriptionManagementContext fsm            = new SubscriptionManagementContext(
                                                                                                   this);
    private SocketChannelHandler                handler;
    private final PushNotification              notification;
    private final Node                          self;

    public SubscriptionManagement(Node node, PushNotification notification) {
        self = node;
        this.notification = notification;
        fsm.setName(String.format("%s<?", self));
    }

    public void connect(SocketChannelHandler handler, Node partner) {
        this.handler = handler;
        fsm.setName(String.format("%s<%s", self, partner));
        fsm.connect();
    }

    /**
     * @return
     */
    public SubscriptionManagementState getState() {
        return fsm.getState();
    }

    public void readReady() {
        fsm.readReady();
    }

    private boolean fillBuffer() {
        int read;
        try {
            read = handler.getChannel().read(eventBuffer);
            if (read < 0) {
                if (log.isInfoEnabled()) {
                    log.info("closing channel");
                }
                error = true;
                return false;
            } else if (eventBuffer.hasRemaining()) {
                // extra read attempt
                int plusRead = handler.getChannel().read(eventBuffer);
                if (plusRead < 0) {
                    if (log.isInfoEnabled()) {
                        log.info("closing channel");
                    }
                    error = true;
                    return false;
                }
                read += plusRead;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isInfoEnabled()) {
                    log.info(String.format("Closing subscription management %s",
                                           fsm.getName()));
                }
            }
            error = true;
            return false;
        }
        return read != 0;
    }

    protected void close() {
        handler.close();
    }

    protected boolean inError() {
        return error;
    }

    protected boolean readMessage() {
        while (fillBuffer()) {
            eventBuffer.flip();
            while (eventBuffer.remaining() >= SubscriptionEvent.BYTE_SIZE) {
                SubscriptionEvent event = new SubscriptionEvent(eventBuffer);
                notification.handle(event);
            }
            eventBuffer.compact();
        }
        return false;
    }

    protected void selectForRead() {
        handler.selectForRead();
    }

}
