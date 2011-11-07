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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
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
    public enum State {
        ERROR, ESTABLISHED, INITIAL, OUTBOUND_HANDSHAKE;
    }

    private static final Logger           log            = Logger.getLogger(Replicator.class.getCanonicalName());

    static final int                      HANDSHAKE_SIZE = Node.BYTE_LENGTH + 4;
    static final int                      MAGIC          = 0x1638;

    private final ReplicatingAppender     appender;
    private final Bundle                  bundle;
    private final Duplicator              duplicator;
    private volatile SocketChannelHandler handler;
    private final ByteBuffer              handshake      = ByteBuffer.allocate(HANDSHAKE_SIZE);
    private volatile Node                 partnerId;
    private final Rendezvous              rendezvous;
    private volatile State                state          = State.INITIAL;

    public Replicator(Bundle bundle, Node partner, Rendezvous rendezvous) {
        duplicator = new Duplicator();
        appender = new ReplicatingAppender(bundle);
        this.bundle = bundle;
        partnerId = partner;
        this.rendezvous = rendezvous;
    }

    public void bindTo(Node node) {
        partnerId = node;
        state = State.ESTABLISHED;
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing(SocketChannel channel) {
        bundle.closeReplicator(partnerId);
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
    public State getState() {
        return state;
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        this.handler = handler;
        switch (state) {
            case ESTABLISHED: {
                duplicator.handleConnect(channel, handler);
                appender.handleAccept(channel, handler);
                try {
                    rendezvous.meet();
                } catch (BrokenBarrierException e) {
                    log.log(Level.WARNING, "Replication rendezvous cancelled",
                            e);
                    handler.close();
                    return;
                }
                break;
            }
            default: {
                log.warning(String.format("Invalid accept state: %s", state));
                close();
            }
        }
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        this.handler = handler;
        switch (state) {
            case INITIAL: {
                handshake.putInt(MAGIC);
                bundle.getId().serialize(handshake);
                handshake.flip();
                state = State.OUTBOUND_HANDSHAKE;
                writeHandshake(channel);
                break;
            }
            default: {
                log.warning(String.format("Invalid connect state: %s", state));
                close();
            }
        }
    }

    @Override
    public void handleRead(SocketChannel channel) {
        switch (state) {
            case ESTABLISHED: {
                appender.handleRead(channel);
                break;
            }
            default: {
                log.warning(String.format("Invalid read state: %s", state));
                close();
            }
        }
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state) {
            case ESTABLISHED: {
                duplicator.handleWrite(channel);
                break;
            }
            case OUTBOUND_HANDSHAKE: {
                writeHandshake(channel);
                break;
            }
            default: {
                log.warning(String.format("Invalid write state: %s", state));
                close();
            }
        }
    }

    public void replicate(ReplicatedBatchHeader header, EventChannel channel,
                          Segment segment, Acknowledger acknowledger) {
        duplicator.replicate(header, channel, segment, acknowledger);
    }

    private void writeHandshake(SocketChannel channel) {
        try {
            channel.write(handshake);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Unable to write handshake from: %s", channel),
                    e);
            state = State.ERROR;
            handler.close();
            return;
        }
        if (handshake.hasRemaining()) {
            handler.selectForWrite();
        } else {
            duplicator.handleConnect(channel, handler);
            appender.handleAccept(channel, handler);
            state = State.ESTABLISHED;
            try {
                rendezvous.meet();
            } catch (BrokenBarrierException e) {
                log.log(Level.WARNING, "Replication rendezvous cancelled", e);
                handler.close();
                return;
            }
        }
    }
}
