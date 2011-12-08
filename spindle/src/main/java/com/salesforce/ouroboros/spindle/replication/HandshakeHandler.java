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
package com.salesforce.ouroboros.spindle.replication;

import static com.salesforce.ouroboros.spindle.replication.Replicator.HANDSHAKE_SIZE;
import static com.salesforce.ouroboros.spindle.replication.Replicator.MAGIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Weaver;

/**
 * 
 * @author hhildebrand
 * 
 */
public class HandshakeHandler implements CommunicationsHandler {
    private static final Logger  log       = Logger.getLogger(HandshakeHandler.class.getCanonicalName());

    private final ByteBuffer     handshake = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private SocketChannelHandler handler;
    private final Weaver         weaver;

    public HandshakeHandler(Weaver weaver) {
        this.weaver = weaver;
    }

    @Override
    public void closing() {
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        readReady();
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readReady() {
        try {
            handler.getChannel().read(handshake);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("unable to read handshake on %s",
                                  handler.getChannel()), e);
            handler.close();
        }
        if (handshake.hasRemaining()) {
            handler.selectForRead();
        } else {
            handshake.flip();
            int magic = handshake.getInt();
            if (MAGIC != magic) {
                log.warning(String.format("Protocol validation error, invalid magic from: %s, received: %s",
                                          handler.getChannel(), magic));
                handler.close();
            } else {
                Node node = new Node(handshake);
                Replicator replicator = weaver.getReplicator(node);
                assert replicator != null : String.format("Replicator for %s does not exist",
                                                          weaver.getId());
                replicator.accept(handler);
            }
        }

    }

    @Override
    public void writeReady() {
        throw new UnsupportedOperationException();
    }

}
