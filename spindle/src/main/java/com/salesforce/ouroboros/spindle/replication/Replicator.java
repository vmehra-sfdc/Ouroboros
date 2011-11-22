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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BrokenBarrierException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.replication.ReplicatorContext.ReplicatorFSM;
import com.salesforce.ouroboros.spindle.replication.ReplicatorContext.ReplicatorState;
import com.salesforce.ouroboros.spindle.source.Acknowledger;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * A full duplex replicator of event streams. The replicator provides both
 * outbound replication of events sourced in the host process as well as
 * accepting replicated events from the mirrored partner process on the same
 * channel.
 * 
 * Replicators have a strict sense of connecting with their mirror process. In
 * order to use both the inbound and outbound streams of the socket, each pair
 * of processes must only connect once. Thus, one process of the mirror pair
 * will initiate the connection and the other pair will accept the new
 * connection. Once the replication connection is established, both sides will
 * replicate events between them.
 * 
 * The Replicator acts as the communication handler which is used to establish
 * the connection between the primary and secondary server and after the
 * connection is established, delegate the event replication transport to the
 * appender or duplicator, depending on whether the communication is inbound or
 * outbound.
 * 
 * @author hhildebrand
 * 
 */
public class Replicator implements CommunicationsHandler {

    private static final Logger       log            = Logger.getLogger(Replicator.class.getCanonicalName());

    public static final int           HANDSHAKE_SIZE = Node.BYTE_LENGTH + 4;
    public static final int           MAGIC          = 0x1638;

    private final ReplicatingAppender appender;
    private final Bundle              bundle;
    private final Duplicator          duplicator;
    private final ReplicatorContext   fsm            = new ReplicatorContext(
                                                                             this);
    private SocketChannelHandler      handler;
    private final ByteBuffer          handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private boolean                   inError;
    private final Node                partnerId;
    private final Rendezvous          rendezvous;

    public Replicator(Bundle bundle, Node partner, Rendezvous rendezvous) {
        duplicator = new Duplicator();
        appender = new ReplicatingAppender(bundle);
        this.bundle = bundle;
        partnerId = partner;
        this.rendezvous = rendezvous;
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
    }

    public void bind() {
        fsm.established();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        bundle.closeReplicator(partnerId);
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.handshake();
    }

    /**
     * @return the appender
     */
    public ReplicatingAppender getAppender() {
        return appender;
    }

    /**
     * @return the duplicator
     */
    public Duplicator getDuplicator() {
        return duplicator;
    }

    public Node getPartnerId() {
        return partnerId;
    }

    /**
     * @return the state
     */
    public ReplicatorState getState() {
        return fsm.getState();
    }

    @Override
    public void readReady() {
        if (fsm.getState() == ReplicatorFSM.Established) {
            appender.readReady();
        }
    }

    public void replicate(ReplicatedBatchHeader header, EventChannel channel,
                          Segment segment, Acknowledger acknowledger) {
        duplicator.replicate(header, channel, segment, acknowledger);
    }

    @Override
    public void writeReady() {
        if (fsm.getState() == ReplicatorFSM.Established) {
            duplicator.writeReady();
        } else {
            fsm.writeReady();
        }
    }

    protected void established() {
        duplicator.connect(handler);
        appender.accept(handler);
        try {
            rendezvous.meet();
        } catch (BrokenBarrierException e) {
            log.log(Level.WARNING, "Replication rendezvous cancelled", e);
            handler.close();
            return;
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected void processHandshake() {
        handshake.putInt(MAGIC);
        bundle.getId().serialize(handshake);
        handshake.flip();

        if (writeHandshake()) {
            fsm.established();
        } else {
            handler.selectForWrite();
        }
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeHandshake() {
        try {
            handler.getChannel().write(handshake);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write handshake from: %s",
                                  handler.getChannel()), e);
            inError = true;
            return false;
        }

        return !handshake.hasRemaining();
    }
}
