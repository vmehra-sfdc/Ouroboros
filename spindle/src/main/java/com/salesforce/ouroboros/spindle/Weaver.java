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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.orchestration.Coordinator;

/**
 * The Weaver represents the channel buffer process that provides persistent,
 * replicated buffers of events, their subscriptions and the services for
 * publishing and consuming these events.
 * 
 * @author hhildebrand
 * 
 */
public class Weaver implements Bundle {

    private class ReplicatorFactory implements
            CommunicationsHandlerFactory<Replicator> {
        @Override
        public Replicator createCommunicationsHandler(SocketChannel channel) {
            try {
                channel.configureBlocking(true);
            } catch (IOException e) {
                String msg = String.format("Unable to configure blocking for socket channel: %s",
                                           channel);
                log.log(Level.WARNING, msg, e);
                throw new IllegalStateException(msg, e);
            }
            Node node = readHandshake(channel);
            if (node == null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    log.log(Level.FINEST,
                            String.format("Error closing socket channel: %s",
                                          channel), e);
                    throw new IllegalStateException(
                                                    String.format("Error reading replication handshake from socket channel: %s",
                                                                  channel));
                }
            }
            Replicator replicator = replicators.get(node);
            replicator.bindTo(node);
            return replicator;
        }

        private Node readHandshake(SocketChannel channel) {
            ByteBuffer handshake = ByteBuffer.allocate(Replicator.HANDSHAKE_SIZE);
            try {
                channel.read(handshake);
            } catch (IOException e) {
                log.log(Level.WARNING,
                        String.format("Unable to read handshake from: %s",
                                      channel), e);
                return null;
            }
            handshake.flip();
            int magic = handshake.getInt();
            if (Replicator.MAGIC != magic) {
                log.warning(String.format("Protocol validation error, invalid magic from: %s, received: %s",
                                          channel, magic));
                return null;
            }
            return new Node(handshake);
        }
    }

    private class SpindleFactory implements
            CommunicationsHandlerFactory<Spinner> {
        @Override
        public Spinner createCommunicationsHandler(SocketChannel channel) {
            return new Spinner(Weaver.this);
        }
    }

    private static final Logger                          log               = Logger.getLogger(Weaver.class.getCanonicalName());
    private static final String                          WEAVER_REPLICATOR = "Weaver Replicator";
    private static final String                          WEAVER_SPINDLE    = "Weaver Spindle";
    private static final String                          WEAVER_XEROX      = "Weaver Xerox";

    private final ConcurrentMap<UUID, EventChannel>      channels          = new ConcurrentHashMap<UUID, EventChannel>();
    private final Node                                   id;
    private final long                                   maxSegmentSize;
    private final ServerSocketChannelHandler<Replicator> replicationHandler;
    private final ConcurrentMap<Node, Replicator>        replicators       = new ConcurrentHashMap<Node, Replicator>();
    private final File                                   root;
    private final ServerSocketChannelHandler<Spinner>    spindleHandler;
    private final ChannelHandler<Xerox>                  xeroxHandler;
    private final Coordinator                            coordinator;

    public Weaver(WeaverConfigation configuration, Coordinator coordinator)
                                                                           throws IOException {
        configuration.validate();
        this.coordinator = coordinator;
        id = configuration.getId();
        root = configuration.getRoot();
        maxSegmentSize = configuration.getMaxSegmentSize();
        xeroxHandler = new ChannelHandler<Xerox>(
                                                 WEAVER_XEROX,
                                                 configuration.getXeroxSocketOptions(),
                                                 configuration.getXeroxes());
        replicationHandler = new ServerSocketChannelHandler<Replicator>(
                                                                        WEAVER_REPLICATOR,
                                                                        configuration.getReplicationSocketOptions(),
                                                                        configuration.getReplicationAddress(),
                                                                        configuration.getReplicators(),
                                                                        new ReplicatorFactory());
        spindleHandler = new ServerSocketChannelHandler<Spinner>(
                                                                 WEAVER_SPINDLE,
                                                                 configuration.getSpindleSocketOptions(),
                                                                 configuration.getSpindleAddress(),
                                                                 configuration.getSpindles(),
                                                                 new SpindleFactory());

        if (!root.exists()) {
            if (!root.mkdirs()) {
                throw new IllegalStateException(
                                                String.format("Cannot create root directory: %s",
                                                              root.getAbsolutePath()));
            }
        }
        if (!root.isDirectory()) {
            throw new IllegalStateException(
                                            String.format("Root is not a directory: %s",
                                                          root.getAbsolutePath()));
        }
        coordinator.ready(this);
    }

    public void close(UUID channel) {
        EventChannel eventChannel = channels.remove(channel);
        if (channel != null) {
            eventChannel.close();
        }
    }

    public void closeReplicator(Node node) {
        Replicator replicator = replicators.remove(node);
        if (replicator != null) {
            replicator.close();
        }
    }

    @Override
    public EventChannel eventChannelFor(EventHeader header) {
        return channels.get(header.getChannel());
    }

    /**
     * Failover any channels that we have been mirroring for the dead members
     * 
     * @param deadMembers
     *            - the weaver nodes that have died
     */
    public void failover(Collection<Node> deadMembers) {
        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            UUID channelId = entry.getKey();
            EventChannel channel = entry.getValue();
            Node[] pair = coordinator.getReplicationPair(channelId);
            if (deadMembers.contains(pair[0]) || deadMembers.contains(pair[1])) {
                if (channel.isPrimary()) {
                    // The mirror for this channel has died
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Mirror for %s has died, old mirror: %s",
                                               channelId, pair[1]));
                    }
                } else {
                    // This node is now the primary for the channel, xerox state to the new mirror
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Weaver[%s] assuming primary role for: %s, old primary: %s",
                                               id, channelId, pair[1]));
                    }
                    channel.setPrimary();
                }
            }
        }
    }

    /**
     * @return the id
     */
    public Node getId() {
        return id;
    }

    /**
     * Add the subscription with the receiver as the mirror for this channel
     * 
     * @param channel
     *            - The new subscription
     * @param primary
     *            - the primary node for this subscription
     */
    public void openMirror(UUID channel, Node primary) {
        // This node is the mirror for the event channel
        if (log.isLoggable(Level.INFO)) {
            log.fine(String.format(" Weaver[%s] is the mirror for the new subscription %s",
                                   id, channel));
        }
        EventChannel ec = new EventChannel(Role.MIRROR, channel, root,
                                           maxSegmentSize,
                                           replicators.get(primary));
        channels.put(channel, ec);
    }

    /**
     * Add the subscription with the receiver as the primary for this channel
     * 
     * @param channel
     *            - the new subscription
     * @param mirror
     *            - the mirror node for this subscription
     */
    public void openPrimary(UUID channel, Node mirror) {
        // This node is the primary for the event channel
        if (log.isLoggable(Level.INFO)) {
            log.fine(String.format(" Weaver[%s] is the primary for the new subscription %s",
                                   id, channel));
        }
        EventChannel ec = new EventChannel(Role.PRIMARY, channel, root,
                                           maxSegmentSize,
                                           replicators.get(mirror));
        channels.put(channel, ec);
    }

    /**
     * Open a replicator to the node
     * 
     * @param node
     *            - the replication node
     * @param info
     *            - the contact information for the node
     * @param latch
     *            - the count down latch to sychronize connectivity
     */
    public void openReplicator(Node node, ContactInformation info,
                               CountDownLatch latch) {
        Replicator replicator = new Replicator(this, node, latch);
        replicators.put(node, replicator);
        if (thisEndInitiatesConnectionsTo(node)) {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Initiating connection from weaver %s to new weaver %s",
                                       id, node));
            }
            try {
                replicationHandler.connectTo(info.replication, replicator);
            } catch (IOException e) {
                // We be screwed.  Log this #fail and force a repartition event
                String msg = String.format("Unable to connect to weaver: %s at replicator port: %s",
                                           node, info);
                log.log(Level.SEVERE, msg, e);
                throw new IllegalStateException(msg, e);
            }
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Waiting for connection to weaver %s from new weaver %s",
                                       id, node));
            }
        }
    }

    /**
     * Rebalance a channel that this node serves as either the primary or
     * mirror. Answer the list of Xerox machines that will perform the state
     * transfer, if needed, between this node and other nodes also responsible
     * for the channel
     * 
     * @param channel
     *            - the id of the channel to rebalance
     * @param pair
     *            - the pair of nodes that are the new primary/mirror
     *            responsible for the channel
     * @return the list of Xerox machines which will perform any necessary state
     *         transfer for the channel
     */
    public List<Xerox> rebalance(UUID channel, Node[] pair,
                                 Collection<Node> deadMembers) {
        EventChannel eventChannel = channels.get(channel);
        assert eventChannel != null : String.format("The event channel to rebalance does not exist: %s",
                                                    channel);
        ArrayList<Xerox> xeroxes = new ArrayList<Xerox>(3);
        if (pair[0].equals(id)) {
            // we are the new primary, rebalancing to the mirror
            log.info(String.format("Rebalancing channel %s on primary: %s to new mirror on: %s",
                                   channel, id, pair[1]));
            xeroxes.add(new Xerox(pair[1], channel,
                                  eventChannel.getSegmentStack()));
        } else if (pair[1].equals(id)) {
            // we are the new secondary, rebalancing to the new primary
            log.info(String.format("Rebalancing channel %s on mirror: %s to new primary on: %s",
                                   channel, id, pair[0]));
            xeroxes.add(new Xerox(pair[0], channel,
                                  eventChannel.getSegmentStack()));
        } else {
            // we are neither the new primary, or the new secondary
            log.info(String.format("Rebalancing channel %s on existing primary: %s to new primary %s, new mirror on: %s",
                                   channel, id, pair[0], pair[1]));
            xeroxes.add(new Xerox(pair[0], channel,
                                  eventChannel.getSegmentStack()));
            xeroxes.add(new Xerox(pair[1], channel,
                                  eventChannel.getSegmentStack()));
        }
        return xeroxes;
    }

    /**
     * Start the weaver
     */
    public void start() {
        spindleHandler.start();
        replicationHandler.start();
        xeroxHandler.start();
    }

    /**
     * Terminate the weaver
     */
    public void terminate() {
        spindleHandler.terminate();
        replicationHandler.terminate();
        xeroxHandler.terminate();
    }

    @Override
    public String toString() {
        return String.format("Weaver[%s], spindle endpoint: %s, replicator endpoint: %s",
                             id, spindleHandler.getLocalAddress(),
                             replicationHandler.getLocalAddress());
    }

    /**
     * indicates which end initiates a connection
     * 
     * @param target
     *            - the other end of the intended connection
     * @return - true if this end initiates the connection, false otherwise
     */
    private boolean thisEndInitiatesConnectionsTo(Node target) {
        return id.compareTo(target) < 0;
    }
}
