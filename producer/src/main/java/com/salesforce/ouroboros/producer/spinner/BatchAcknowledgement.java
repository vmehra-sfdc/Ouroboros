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
package com.salesforce.ouroboros.producer.spinner;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.producer.spinner.BatchAcknowledgementContext.BatchAcknowledgementState;
import com.salesforce.ouroboros.util.Utils;

/**
 * The state machine implementing the batch event acknowledgement protocol
 * 
 * @author hhildebrand
 * 
 */
public class BatchAcknowledgement {
    private final static int                  MAX_ACK_BATCH = 1000;
    private final static Logger               log           = LoggerFactory.getLogger(BatchAcknowledgement.class.getCanonicalName());

    private final ByteBuffer                  ackBuffer     = ByteBuffer.allocate(BatchIdentity.BYTE_SIZE
                                                                                  * MAX_ACK_BATCH);
    private final BatchAcknowledgementContext fsm           = new BatchAcknowledgementContext(
                                                                                              this);
    private SocketChannelHandler              handler;
    private boolean                           inError       = false;
    private final Spinner                     spinner;

    public BatchAcknowledgement(Spinner spinner, String fsmName) {
        this.spinner = spinner;
        fsm.setName(fsmName);
    }

    public void closing() {
        if (!fsm.isInTransition()) {
            fsm.close();
        }
    }

    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.connect();
    }

    public void failover() {
        fsm.close();
    }

    public BatchAcknowledgementState getState() {
        return fsm.getState();
    }

    public void readReady() {
        fsm.readReady();
    }

    private boolean readAcknowledgement() {
        int read;
        try {
            read = handler.getChannel().read(ackBuffer);
            if (read < 0) {
                if (log.isInfoEnabled()) {
                    log.info("closing channel");
                }
                inError = true;
                return false;
            } else if (ackBuffer.hasRemaining()) {
                // extra read attempt
                int plusRead = handler.getChannel().read(ackBuffer);
                if (plusRead < 0) {
                    if (log.isInfoEnabled()) {
                        log.info("closing channel");
                    }
                    inError = true;
                    return false;
                }
                read += plusRead;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isInfoEnabled()) {
                    log.info("Closing batch acknowlegement");
                }
            }
            inError = true;
            return false;
        }
        return read != 0;
    }

    protected void close() {
        if (handler != null) {
            handler.close();
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected boolean readAcknowledgements() {
        while (readAcknowledgement()) {
            ackBuffer.flip();
            while (ackBuffer.remaining() >= BatchIdentity.BYTE_SIZE) {
                BatchIdentity ack = new BatchIdentity(ackBuffer);
                spinner.acknowledge(ack);
            }
            ackBuffer.compact();
        }
        return false;
    }

    protected void selectForRead() {
        handler.selectForRead();
    }
}
