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

import static com.salesforce.ouroboros.util.Utils.point;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.WeaverConfigation.RootDirectory;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * The Weaver represents the channel buffer process that provides persistent,
 * replicated buffers of events, their subscriptions and the services for
 * publishing and consuming these events.
 * 
 * @author hhildebrand
 * 
 */
public class Weaver implements Bundle {

    private class ReplicatorFactory implements CommunicationsHandlerFactory {
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
            replicator.bind();
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

    private class SpindleFactory implements CommunicationsHandlerFactory {
        @Override
        public Spindle createCommunicationsHandler(SocketChannel channel) {
            return new Spindle(Weaver.this);
        }
    }

    private static final Logger                     log               = Logger.getLogger(Weaver.class.getCanonicalName());
    private static final String                     WEAVER_REPLICATOR = "Weaver Replicator";
    private static final String                     WEAVER_SPINDLE    = "Weaver Spindle";
    private static final String                     WEAVER_XEROX      = "Weaver Xerox";

    private final ConcurrentMap<Node, Acknowledger> acknowledgers     = new ConcurrentHashMap<Node, Acknowledger>();
    private final ConcurrentMap<UUID, EventChannel> channels          = new ConcurrentHashMap<UUID, EventChannel>();
    private final ContactInformation                contactInfo;
    private Coordinator                             coordinator;
    private final Node                              id;
    private final long                              maxSegmentSize;
    private final ServerSocketChannelHandler        replicationHandler;
    private final ConcurrentMap<Node, Replicator>   replicators       = new ConcurrentHashMap<Node, Replicator>();
    private final ConsistentHashFunction<File>      roots             = new ConsistentHashFunction<File>();
    private final ServerSocketChannelHandler        spindleHandler;
    private final ChannelHandler                    xeroxHandler;

    public Weaver(WeaverConfigation configuration) throws IOException {
        configuration.validate();
        id = configuration.getId();
        for (RootDirectory root : configuration.getRoots()) {
            roots.add(root.directory, root.weight);
            if (!root.directory.exists()) {
                if (!root.directory.mkdirs()) {
                    throw new IllegalStateException(
                                                    String.format("Cannot create root directory: %s",
                                                                  root.directory.getAbsolutePath()));
                }
            }
            if (!root.directory.isDirectory()) {
                throw new IllegalStateException(
                                                String.format("Root is not a directory: %s",
                                                              root.directory.getAbsolutePath()));
            }
        }
        maxSegmentSize = configuration.getMaxSegmentSize();
        xeroxHandler = new ChannelHandler(
                                          WEAVER_XEROX,
                                          configuration.getXeroxSocketOptions(),
                                          configuration.getXeroxes());
        replicationHandler = new ServerSocketChannelHandler(
                                                            WEAVER_REPLICATOR,
                                                            configuration.getReplicationSocketOptions(),
                                                            configuration.getReplicationAddress(),
                                                            configuration.getReplicators(),
                                                            new ReplicatorFactory());
        spindleHandler = new ServerSocketChannelHandler(
                                                        WEAVER_SPINDLE,
                                                        configuration.getSpindleSocketOptions(),
                                                        configuration.getSpindleAddress(),
                                                        configuration.getSpindles(),
                                                        new SpindleFactory());
        contactInfo = new ContactInformation(
                                             spindleHandler.getLocalAddress(),
                                             replicationHandler.getLocalAddress(),
                                             null);
    }

    public void close(UUID channel) {
        EventChannel eventChannel = channels.remove(channel);
        if (channel != null) {
            eventChannel.close();
        }
    }

    @Override
    public void closeReplicator(Node node) {
        Replicator replicator = replicators.remove(node);
        if (replicator != null) {
            replicator.close();
        }
    }

    @Override
    public EventChannel eventChannelFor(UUID channelId) {
        return channels.get(channelId);
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

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Bundle#getAcknowledger(com.salesforce.ouroboros.Node)
     */
    @Override
    public Acknowledger getAcknowledger(Node node) {
        return acknowledgers.get(node);
    }

    public ContactInformation getContactInformation() {
        return contactInfo;
    }

    /**
     * @return the id
     */
    @Override
    public Node getId() {
        return id;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Bundle#map(com.salesforce.ouroboros.Node, com.salesforce.ouroboros.spindle.Acknowledger)
     */
    @Override
    public void map(Node producer, Acknowledger acknowledger) {
        acknowledgers.put(producer, acknowledger);
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
        EventChannel ec = new EventChannel(Role.MIRROR, channel,
                                           roots.hash(point(channel)),
                                           maxSegmentSize,
                                           replicators.get(primary));
        channels.putIfAbsent(channel, ec);
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
        EventChannel ec = new EventChannel(Role.PRIMARY, channel,
                                           roots.hash(point(channel)),
                                           maxSegmentSize,
                                           replicators.get(mirror));
        channels.putIfAbsent(channel, ec);
    }

    /**
     * Open a replicator to the node
     * 
     * @param node
     *            - the replication node
     * @param info
     *            - the contact information for the node
     * @param rendezvous
     *            - the rendezvous used to sychronize connectivity
     */
    public void openReplicator(Node node, ContactInformation info,
                               Rendezvous rendezvous) {
        Replicator replicator = new Replicator(this, node, rendezvous);
        Replicator previous = replicators.putIfAbsent(node, replicator);
        assert previous == null : String.format("Replicator already opend on weaver %s to weaver %s");
        if (thisEndInitiatesConnectionsTo(node)) {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Initiating replication connection from weaver %s to new weaver %s",
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
                log.fine(String.format("Waiting for replication inbound connection to weaver %s from new weaver %s",
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
     * @param remappedPair
     *            - the pair of nodes that are the new primary/mirror
     *            responsible for the channel
     * @return the list of Xerox machines which will perform any necessary state
     *         transfer for the channel
     */
    public List<Xerox> rebalance(UUID channel, Node[] originalPair,
                                 Node[] remappedPair,
                                 Collection<Node> deadMembers) {
        EventChannel eventChannel = channels.get(channel);
        assert eventChannel != null : String.format("The event channel to rebalance does not exist: %s",
                                                    channel);
        if (eventChannel.isPrimary()) {
            // self is the primary
            if (deadMembers.contains(originalPair[1])) {
                // mirror is down
                if (id.equals(remappedPair[0])) {
                    // Xerox state to the new mirror
                    return Arrays.asList(new Xerox(
                                                   remappedPair[1],
                                                   channel,
                                                   eventChannel.getSegmentStack()));
                }
                // Xerox state to new primary and mirror
                return Arrays.asList(new Xerox(remappedPair[0], channel,
                                               eventChannel.getSegmentStack()),
                                     new Xerox(remappedPair[1], channel,
                                               eventChannel.getSegmentStack()));
            }
            // mirror is up
            if (!id.equals(remappedPair[0])) {
                // Xerox state to the new primary
                return Arrays.asList(new Xerox(remappedPair[0], channel,
                                               eventChannel.getSegmentStack()));
            }

            return null; // Nothing to do
        }

        // self is the secondary
        if (deadMembers.contains(originalPair[0])) {
            // primary is down
            if (id.equals(remappedPair[1])) {
                // Xerox state to the new primary
                return Arrays.asList(new Xerox(remappedPair[0], channel,
                                               eventChannel.getSegmentStack()));
            }
            // Xerox state to the new primary and mirror
            return Arrays.asList(new Xerox(remappedPair[0], channel,
                                           eventChannel.getSegmentStack()),
                                 new Xerox(remappedPair[1], channel,
                                           eventChannel.getSegmentStack()));
        }

        // primary is up
        if (!id.equals(remappedPair[1])) {
            // Xerox state to the new mirror
            return Arrays.asList(new Xerox(remappedPair[1], channel,
                                           eventChannel.getSegmentStack()));
        }

        return null; // nothing to do
    }

    public void setCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
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
