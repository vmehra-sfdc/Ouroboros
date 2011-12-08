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
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.ContactInformation;
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

    public static final int         HANDSHAKE_SIZE = Node.BYTE_LENGTH + 4;

    public static final int         MAGIC          = 0x1638;
    private static final Logger     log            = Logger.getLogger(Replicator.class.getCanonicalName());

    private ReplicatingAppender     appender;
    private final Bundle            bundle;
    private final Duplicator        duplicator     = new Duplicator();
    private final ReplicatorContext fsm            = new ReplicatorContext(this);
    private SocketChannelHandler    handler;
    private final ByteBuffer        handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private boolean                 inError;
    private final Node              partner;
    private final Rendezvous        rendezvous;

    public Replicator(Bundle bundle, Node partner, boolean originator,
                      Rendezvous rendezvous) {
        if (originator) {
            appender = new ReplicatingAppender(bundle);
        }
        this.bundle = bundle;
        this.partner = partner;
        this.rendezvous = rendezvous;
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        assert !willOriginate() : "This replicator does not accept connections";
        this.handler = handler;
        appender = new ReplicatingAppender(bundle);
        handler.resetHandler(this);
        fsm.established();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        bundle.closeReplicator(partner);
    }

    public void connect(Map<Node, ContactInformation> yellowPages,
                        ServerSocketChannelHandler handler) throws IOException {
        assert willOriginate() : "This replicator does not originate connections";
        ContactInformation contactInformation = yellowPages.get(partner);
        assert contactInformation != null : String.format("Contact information for %s is missing",
                                                          partner);
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Initiating replication connection to new weaver %s",
                                   partner));
        }
        handler.connectTo(contactInformation.replication, this);
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        assert willOriginate() : "This replicator does not originate connections";
        this.handler = handler;
        fsm.handshake();
    }

    public Node getPartner() {
        return partner;
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

    /**
     * @return true if the receiver is not connected and originates connections
     */
    public boolean willOriginate() {
        return handler == null && appender != null;
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
            log.log(Level.WARNING,
                    String.format("Replication rendezvous has been previously cancelled on %s",
                                  bundle.getId()), e);
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
