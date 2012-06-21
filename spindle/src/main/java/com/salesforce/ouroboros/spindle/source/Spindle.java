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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.batch.BatchWriter;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.source.SpindleContext.SpindleFSM;
import com.salesforce.ouroboros.spindle.source.SpindleContext.SpindleState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The communications wrapper that ties together the appender and the
 * acknowledger.
 * 
 * @author hhildebrand
 * 
 */
public class Spindle implements CommunicationsHandler {

    public static final Integer      HANDSHAKE_SIZE = Node.BYTE_LENGTH + 4;
    public final static int          MAGIC          = 0x1638;
    private final static Logger      log            = LoggerFactory.getLogger(Spindle.class.getCanonicalName());

    private final Bundle             bundle;
    private final SpindleContext     fsm            = new SpindleContext(this);
    private SocketChannelHandler     handler;
    private ByteBuffer               handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private boolean                  inError;
    final BatchWriter<BatchIdentity> acknowledger;
    final Appender                   appender;
    private Node                     producer;

    public Spindle(Bundle bundle) {
        fsm.setName(String.format("?>%s", bundle.getId().processId));
        String ackFsmName = String.format("Acknowledger[?<%s]", bundle.getId());
        acknowledger = new BatchWriter<BatchIdentity>(BatchIdentity.BYTE_SIZE,
                                                      1000, ackFsmName);
        appender = new Appender(bundle, acknowledger);
        this.bundle = bundle;
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        acknowledger.connect(handler);
        appender.accept(handler);
        fsm.handshake();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        acknowledger.closing();
        appender.closing();
        bundle.closeAcknowledger(producer);
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public SpindleState getState() {
        return fsm.getState();
    }

    public boolean isEstablished() {
        return fsm.getState() == SpindleFSM.Established;
    }

    @Override
    public void readReady() {
        if (fsm.getState() == SpindleFSM.Established) {
            appender.readReady();
        } else {
            fsm.readReady();
        }
    }

    @Override
    public void writeReady() {
        acknowledger.writeReady();
    }

    protected void established() {
        handshake.flip();
        int magic = handshake.getInt();
        if (magic != MAGIC) {
            inError = true;
            log.warn(String.format("Invalid handshake magic: %s", magic));
            handshake = null;
            close();
            return;
        }
        producer = new Node(handshake);
        String fsmName = String.format("%s>%s", producer.processId,
                                       bundle.getId().processId);
        fsm.setName(fsmName);
        appender.setFsmName(fsmName);
        acknowledger.setFsmName(String.format("%s<%s", producer.processId,
                                              bundle.getId().processId));
        bundle.map(producer, acknowledger);
        if (log.isInfoEnabled()) {
            log.info(String.format("Established on %s for %s", bundle.getId(),
                                   producer));
        }
        handshake = null;
        handler.selectForRead();
    }

    protected boolean inError() {
        return inError;
    }

    protected void processHandshake() {
        if (readHandshake()) {
            fsm.established();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForRead();
            }
        }
    }

    protected boolean readHandshake() {
        try {
            if (handler.getChannel().read(handshake) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            inError = true;
            if (Utils.isClose(e)) {
                log.info(String.format("closing Spindle %s ", fsm.getName()));
            } else {
                log.warn(String.format("Error reading handshake"), e);
            }
            return false;
        }
        return !handshake.hasRemaining();
    }

    protected void selectForRead() {
        handler.selectForRead();
    }
}