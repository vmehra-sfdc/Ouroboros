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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.replication.ReplicatorContext.ReplicatorFSM;
import com.salesforce.ouroboros.spindle.replication.ReplicatorContext.ReplicatorState;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

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
    private static final Logger     log            = LoggerFactory.getLogger(Replicator.class.getCanonicalName());

    private ReplicatingAppender     appender;
    private final Bundle            bundle;
    private final Duplicator        duplicator;
    private final ReplicatorContext fsm            = new ReplicatorContext(this);
    private SocketChannelHandler    handler;
    private ByteBuffer              handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private boolean                 inError;
    private Node                    partner;
    private Rendezvous              rendezvous;

    public Replicator(Bundle bundle) {
        fsm.setName(Integer.toString(bundle.getId().processId));
        duplicator = new Duplicator(bundle.getId());
        this.bundle = bundle;
    }

    public Replicator(Bundle bundle, Node partner, Rendezvous rendezvous) {
        this(bundle);
        appender = new ReplicatingAppender(bundle);
        this.partner = partner;
        this.rendezvous = rendezvous;
        duplicator.setFsmName(String.format("%s>%s", bundle.getId().processId,
                                            partner.processId));
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        assert this.handler == null : "This replicator has already been established";
        assert appender == null : "This replicator does not accept handshakes";
        appender = new ReplicatingAppender(bundle);
        this.handler = handler;
        fsm.acceptHandshake();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        duplicator.closing();
        appender.closing();
        bundle.closeReplicator(partner);
    }

    public void connect(Map<Node, ContactInformation> yellowPages,
                        ServerSocketChannelHandler handler) throws IOException {
        assert this.handler == null && appender != null : "This replicator does not originate connections";
        ContactInformation contactInformation = yellowPages.get(partner);
        assert contactInformation != null : String.format("Contact information for %s is missing",
                                                          partner);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Initiating replication connection from %s to new weaver %s",
                                    bundle.getId(), partner));
        }
        handler.connectTo(contactInformation.replication, this);
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        assert this.handler == null && appender != null : "This replicator does not originate connections";
        this.handler = handler;
        if (log.isTraceEnabled()) {
            log.trace(String.format("Starting handshake from %s to %s",
                                    bundle.getId(), partner));
        }
        fsm.initiateHandshake();
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
        } else {
            fsm.readReady();
        }
    }

    public void replicate(EventEntry event) {
        duplicator.replicate(event);
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
        if (rendezvous != null) {
            try {
                rendezvous.meet();
            } catch (BrokenBarrierException e) {
                log.warn(String.format("Replication rendezvous has been previously cancelled on %s",
                                       bundle.getId()), e);
                handler.close();
                return;
            }
        }
        handler.selectForRead();
    }

    protected void inboundHandshake() {
        if (readHandshake()) {
            fsm.established();
        } else {
            handler.selectForRead();
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected void outboundHandshake() {
        handshake.putInt(MAGIC);
        bundle.getId().serialize(handshake);
        handshake.flip();

        if (writeHandshake()) {
            fsm.established();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
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
            if (Utils.isClose(e)) {
                log.info(String.format("closing replicator %s>%s",
                                       bundle.getId(), partner));
            } else {
                log.warn(String.format("unable to read handshake on %s",
                                       handler.getChannel()), e);
            }
            inError = true;
            return false;
        }
        if (handshake.hasRemaining()) {
            return false;
        }
        handshake.flip();
        int magic = handshake.getInt();
        if (MAGIC != magic) {
            log.warn(String.format("Protocol validation error, invalid magic from: %s, received: %s",
                                   handler.getChannel(), magic));
            inError = true;
            return false;
        }
        partner = new Node(handshake);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Inbound handshake completed on %s, partner: %s",
                                    bundle.getId(), partner));
        }
        bundle.map(partner, this);
        duplicator.setFsmName(String.format("%s>%s", bundle.getId().processId,
                                            partner.processId));
        handshake = null;
        return true;
    }

    protected void selectForRead() {
        handler.selectForRead();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeHandshake() {
        try {
            if (handler.getChannel().write(handshake) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing replicator %s>%s",
                                       bundle.getId(), partner));
            } else {
                log.warn(String.format("Unable to write handshake from: %s",
                                       handler.getChannel()), e);
            }
            inError = true;
            return false;
        }

        if (!handshake.hasRemaining()) {
            handshake = null;
            return true;
        }
        return false;
    }
}
