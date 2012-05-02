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
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.WeaverConfigation.RootDirectory;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.source.Acknowledger;
import com.salesforce.ouroboros.spindle.source.Spindle;
import com.salesforce.ouroboros.spindle.transfer.Sink;
import com.salesforce.ouroboros.spindle.transfer.Xerox;
import com.salesforce.ouroboros.spindle.util.ConcurrentLinkedHashMap.Builder;
import com.salesforce.ouroboros.spindle.util.EvictionListener;
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
public class Weaver implements Bundle, Comparable<Weaver> {
    private class ReplicatorFactory implements CommunicationsHandlerFactory {
        @Override
        public Replicator createCommunicationsHandler(SocketChannel channel) {
            return new Replicator(Weaver.this);
        }
    }

    private class SinkFactory implements CommunicationsHandlerFactory {
        @Override
        public Sink createCommunicationsHandler(SocketChannel channel) {
            return new Sink(Weaver.this);
        }
    }

    private class SpindleFactory implements CommunicationsHandlerFactory {
        @Override
        public Spindle createCommunicationsHandler(SocketChannel channel) {
            return new Spindle(Weaver.this, maxEventEntryPoolSize);
        }
    }

    private static final Logger                     log               = LoggerFactory.getLogger(Weaver.class.getCanonicalName());

    private static final String                     WEAVER_REPLICATOR = "Weaver Replicator";
    private static final String                     WEAVER_SPINDLE    = "Weaver Spindle";
    private static final String                     WEAVER_XEROX      = "Weaver Xerox";
    static final int                                HANDSHAKE_SIZE    = Node.BYTE_LENGTH + 4;
    static final int                                MAGIC             = 0x1638;
    private final ConcurrentMap<Node, Acknowledger> acknowledgers     = new ConcurrentHashMap<Node, Acknowledger>();

    private final ConcurrentMap<File, Segment>      appendSegmentCache;
    private final ConcurrentMap<UUID, EventChannel> channels          = new ConcurrentHashMap<UUID, EventChannel>();
    private final ContactInformation                contactInfo;
    private int                                     maxEventEntryPoolSize;
    private final long                              maxSegmentSize;
    private ConsistentHashFunction<Node>            nextRing;
    private final ConcurrentMap<File, Segment>      readSegmentCache;
    private final ServerSocketChannelHandler        replicationHandler;
    private final ConcurrentMap<Node, Replicator>   replicators       = new ConcurrentHashMap<Node, Replicator>();
    private final ConsistentHashFunction<File>      roots;
    private final Node                              self;
    private final ServerSocketChannelHandler        spindleHandler;
    private ConsistentHashFunction<Node>            weaverRing;
    private final ServerSocketChannelHandler        xeroxHandler;

    public Weaver(WeaverConfigation configuration) throws IOException {
        configuration.validate();
        Builder<File, Segment> builder = new Builder<File, Segment>();
        appendSegmentCache = createAppendSegmentCache(configuration, builder);
        readSegmentCache = createReadSegmentCache(configuration, builder);
        self = configuration.getId();
        roots = new ConsistentHashFunction<File>(
                                                 configuration.getRootSkipStrategy(),
                                                 configuration.getNumberOfRootReplicas());
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
        xeroxHandler = new ServerSocketChannelHandler(
                                                      WEAVER_XEROX,
                                                      configuration.getXeroxSocketOptions(),
                                                      configuration.getXeroxAddress(),
                                                      configuration.getXeroxes(),
                                                      new SinkFactory());
        contactInfo = new ContactInformation(
                                             spindleHandler.getLocalAddress(),
                                             replicationHandler.getLocalAddress(),
                                             xeroxHandler.getLocalAddress());
        weaverRing = new ConsistentHashFunction<Node>(
                                                      configuration.getSkipStrategy(),
                                                      configuration.getNumberOfReplicas());
        maxEventEntryPoolSize = configuration.getMaxEventEntryPoolSize();
    }

    public void bootstrap(Node[] bootsrappingMembers) {
        for (Node node : bootsrappingMembers) {
            weaverRing.add(node, node.capacity);
        }
    }

    public void close(UUID channel) {
        EventChannel eventChannel = channels.remove(channel);
        if (channel != null) {
            eventChannel.close(self);
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Bundle#removeAcknowledger(com.salesforce.ouroboros.Node)
     */
    @Override
    public void closeAcknowledger(Node producer) {
        Acknowledger ack = acknowledgers.remove(producer);
        if (ack != null) {
            ack.close();
        }
    }

    @Override
    public void closeReplicator(Node node) {
        Replicator replicator = replicators.remove(node);
        if (replicator != null) {
            replicator.close();
        }
    }

    public void commitRing() {
        weaverRing = nextRing;
        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            UUID channelId = entry.getKey();
            Node[] pair = getReplicationPair(channelId);
            EventChannel channel = entry.getValue();
            if (channel != null) {
                if (!pair[0].equals(self) && !pair[1].equals(self)) {
                    if (log.isInfoEnabled()) {
                        log.info(String.format("Rebalancing, closing channel %s on %s",
                                               channelId, self));
                    }
                    channels.remove(channelId);
                    channel.close(self);
                }
            }
        }
        nextRing = null;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Weaver weaver) {
        return self.compareTo(weaver.self);
    }

    /**
     * Connect the originating replicators using the yellow pages to determine
     * endpoints
     * 
     * @param replicators
     *            - the collection of replicators to connnect
     * 
     * @param yellowPages
     *            - the contact information for the nodes
     */
    public void connectReplicators(Collection<Replicator> replicators,
                                   Map<Node, ContactInformation> yellowPages) {
        for (Replicator replicator : replicators) {
            try {
                replicator.connect(yellowPages, replicationHandler);
            } catch (IOException e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Error connecting originating replicator from %s to %s",
                                           self, replicator.getPartner()), e);
                }
            }
        }
    }

    /**
     * Connnect and start the xeroxes.
     * 
     * @param xeroxes
     * @param yellowPages
     * @param rendezvous
     */
    public void connectXeroxes(Map<Node, Xerox> xeroxes,
                               Map<Node, ContactInformation> yellowPages,
                               Rendezvous rendezvous) {
        for (Map.Entry<Node, Xerox> entry : xeroxes.entrySet()) {
            try {
                Xerox xerox = entry.getValue();
                xerox.setRendezvous(rendezvous);
                xeroxHandler.connectTo(yellowPages.get(entry.getKey()).xerox,
                                       xerox);
            } catch (IOException e) {
                if (log.isInfoEnabled()) {
                    log.info(String.format("Error connecting xerox from %s to %s",
                                           self, entry.getKey()), e);
                }
                rendezvous.cancel();
                return;
            }
        }
    }

    /**
     * @return
     */
    public ConsistentHashFunction<Node> createRing() {
        return new ConsistentHashFunction<Node>(weaverRing.getSkipStrategy(),
                                                weaverRing.replicaePerBucket);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Weaver) {
            return self.equals(((Weaver) o).self);
        }
        return false;
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
        for (Node node : deadMembers) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Removing %s from the partition on %s",
                                       node, self));
            }
            closeAcknowledger(node);
            closeReplicator(node);
        }
        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            UUID channelId = entry.getKey();
            EventChannel channel = entry.getValue();
            if (channel.isPrimary()
                && deadMembers.contains(channel.getPartnerId())) {
                channel.failMirror();
            } else if (deadMembers.contains(channel.getPartnerId())) {
                assert channel.isMirror() : String.format("%s thinks it is the primary for % when it should be the mirror",
                                                          self, channel.getId());
                // This node is now the primary for the channel, xerox state to the new mirror
                if (log.isInfoEnabled()) {
                    log.info(String.format("%s assuming primary role for: %s, old primary: %s",
                                           self, channelId,
                                           channel.getPartnerId()));
                }
                channel.failOver();
            }
        }

        // Clean up the ring
        for (Node node : weaverRing.getBuckets()) {
            if (deadMembers.contains(node)) {
                node.markAsDown();
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
        return self;
    }

    /**
     * Answer the replication pair of nodes that provide the primary and mirror
     * for the channel
     * 
     * @param channel
     *            - the id of the channel
     * @return the tuple of primary and mirror nodes for this channel
     */
    public Node[] getReplicationPair(UUID channel) {
        List<Node> pair = weaverRing.hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    public Replicator getReplicator(Node node) {
        return replicators.get(node);
    }

    @Override
    public int hashCode() {
        return self.hashCode();
    }

    public void inactivate() {
        spindleHandler.closeOpenHandlers();
        replicationHandler.closeOpenHandlers();
        xeroxHandler.closeOpenHandlers();
        for (EventChannel channel : channels.values()) {
            channel.close(self);
        }
        channels.clear();
        replicators.clear();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Bundle#map(com.salesforce.ouroboros.Node, com.salesforce.ouroboros.spindle.Acknowledger)
     */
    @Override
    public void map(Node producer, Acknowledger acknowledger) {
        acknowledgers.put(producer, acknowledger);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Bundle#map(com.salesforce.ouroboros.Node, com.salesforce.ouroboros.spindle.replication.Replicator)
     */
    @Override
    public void map(Node partner, Replicator replicator) {
        Replicator previous = replicators.put(partner, replicator);
        assert previous == null : String.format("Replicator has already been mapped to %s on %s",
                                                partner, self);
    }

    /**
     * Add the subscription with the receiver as the mirror for this channel
     * 
     * @param channel
     *            - The new subscription
     * @param pair
     */
    public void openMirror(UUID channel, Node primary) {
        if (channels.get(channel) != null) {
            return;
        }
        if (log.isInfoEnabled()) {
            log.info(String.format(" %s is the mirror for the new subscription %s",
                                   self, channel));
        }
        channels.put(channel,
                     new EventChannel(self, Role.MIRROR, primary, channel,
                                      roots.hash(point(channel)),
                                      maxSegmentSize, null, appendSegmentCache,
                                      readSegmentCache));
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
        if (channels.get(channel) != null) {
            return;
        }
        // This node is the primary for the event channel
        if (log.isInfoEnabled()) {
            log.info(String.format("%s is the primary for the new subscription %s",
                                   self, channel));
        }
        Replicator replicator = null;
        if (mirror != null) {
            replicator = replicators.get(mirror);
        }
        channels.put(channel,
                     new EventChannel(self, Role.PRIMARY, mirror, channel,
                                      roots.hash(point(channel)),
                                      maxSegmentSize, replicator,
                                      appendSegmentCache, readSegmentCache));
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
     * @return the Replicator to the node
     */
    public Replicator openReplicator(Node node, ContactInformation info,
                                     Rendezvous rendezvous) {
        Replicator replicator = new Replicator(this, node, rendezvous);
        Replicator previous = replicators.putIfAbsent(node, replicator);
        assert previous == null : String.format("Replicator already opend on weaver %s to weaver %s",
                                                self, node);
        return replicator;
    }

    /**
     * Rebalance a channel that this node serves as either the primary or
     * mirror. Add the list of channels to the Xerox machines that will perform
     * the state transfer, if needed, between this node and other nodes also
     * responsible for the channel
     * 
     * @param xeroxes
     *            - the map of Nodes to Xeroxes
     * 
     * @param channel
     *            - the id of the channel to rebalance
     * @param remappedPair
     *            - the pair of nodes that are the new primary/mirror
     *            responsible for the channel
     */
    public void rebalance(Map<Node, Xerox> xeroxes, UUID channel,
                          Node originalPrimary, Node originalMirror,
                          Node remappedPrimary, Node remappedMirror,
                          Collection<Node> activeMembers,
                          WeaverCoordinator coordinator) {
        EventChannel eventChannel = channels.get(channel);
        assert eventChannel != null : String.format("The event channel to rebalance does not exist: %s",
                                                    channel);
        if (self.equals(originalPrimary)) {
            // self is the primary
            if (!activeMembers.contains(originalMirror)) {
                // mirror is down
                if (self.equals(remappedPrimary)) {
                    // if self is still the primary
                    // Xerox state to the new mirror
                    infoLog("Rebalancing for %s from primary %s to new mirror %s",
                            eventChannel.getId(), self, remappedMirror);
                    xeroxTo(eventChannel, remappedMirror, xeroxes, coordinator);
                } else if (self.equals(remappedMirror)) {
                    // Self becomes the new mirror
                    infoLog("Rebalancing for %s, %s becoming mirror from primary, new primary %s",
                            eventChannel.getId(), self, remappedPrimary);
                    eventChannel.rebalanceAsMirror();
                    xeroxTo(eventChannel, remappedPrimary, xeroxes, coordinator);
                } else {
                    // Xerox state to new primary and mirror
                    infoLog("Rebalancing for %s from old primary %s to new primary %s, new mirror %s",
                            eventChannel.getId(), self, remappedPrimary,
                            remappedMirror);
                    xeroxTo(eventChannel, remappedPrimary, xeroxes, coordinator);
                    xeroxTo(eventChannel, remappedMirror, xeroxes, coordinator);
                }
            } else if (!self.equals(remappedPrimary)) {
                // mirror is up
                // Xerox state to the new primary
                infoLog("Rebalancing for %s from old primary %s to new primary %s",
                        eventChannel.getId(), self, remappedPrimary);
                xeroxTo(eventChannel, remappedPrimary, xeroxes, coordinator);
                if (self.equals(remappedMirror)) {
                    eventChannel.rebalanceAsMirror();
                }
            }
        } else if (!activeMembers.contains(originalPrimary)) {
            assert self.equals(originalMirror);
            // self is the secondary
            // primary is down
            if (self.equals(remappedMirror)) {
                // Self is still the mirror
                // Xerox state to the new primary
                infoLog("Rebalancing for %s from mirror %s to new primary %s",
                        eventChannel.getId(), self, remappedPrimary);
                xeroxTo(eventChannel, remappedPrimary, xeroxes, coordinator);
            } else if (self.equals(remappedPrimary)) {
                // Self becomes the new primary
                infoLog("Rebalancing for %s, %s becoming primary from mirror, new mirror %s",
                        eventChannel.getId(), self, remappedMirror);
                eventChannel.rebalanceAsPrimary(remappedMirror,
                                                replicators.get(remappedMirror),
                                                self);
                xeroxTo(eventChannel, remappedMirror, xeroxes, coordinator);
            } else {
                // Xerox state to the new primary and mirror
                infoLog("Rebalancing for %s from old mirror %s to new primary %s, new mirror %s",
                        eventChannel.getId(), self, remappedPrimary,
                        remappedMirror);
                xeroxTo(eventChannel, remappedPrimary, xeroxes, coordinator);
                xeroxTo(eventChannel, remappedMirror, xeroxes, coordinator);
            }
        } else if (!self.equals(remappedMirror)
                   && !remappedMirror.equals(originalPrimary)) {
            assert self.equals(originalMirror);
            // primary is up
            // Xerox state to the new mirror
            infoLog("Rebalancing for %s from old mirror %s to new mirror %s",
                    eventChannel.getId(), self, remappedMirror);
            xeroxTo(eventChannel, remappedMirror, xeroxes, coordinator);
        }
    }

    /**
     * Set the consistent hash function for the next weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    public void setNextRing(ConsistentHashFunction<Node> ring) {
        nextRing = ring;
    }

    public void setRing(ConsistentHashFunction<Node> nextRing) {
        weaverRing = nextRing;
    }

    /**
     * Start the weaver
     */
    @PostConstruct
    public void start() {
        spindleHandler.start();
        replicationHandler.start();
        xeroxHandler.start();
    }

    /**
     * Terminate the weaver
     */
    @PreDestroy
    public void terminate() {
        spindleHandler.terminate();
        replicationHandler.terminate();
        xeroxHandler.terminate();
        for (EventChannel channel : channels.values()) {
            channel.close(self);
        }
    }

    @Override
    public String toString() {
        return String.format("Weaver %s, spindle endpoint: %s, replicator endpoint: %s",
                             self, spindleHandler.getLocalAddress(),
                             replicationHandler.getLocalAddress());
    }

    @Override
    public EventChannel xeroxEventChannel(UUID channel) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("%s created a new channel for %s", self,
                                    channel));
        }

        List<Node> pair = nextRing.hash(point(channel), 2);
        EventChannel ec;
        if (self.equals(pair.get(0))) {
            Replicator replicator = replicators.get(pair.get(0));
            assert replicator == null : String.format("Replicator for %s is null on %s",
                                                      channel, self);
            ec = new EventChannel(self, Role.PRIMARY, pair.get(1), channel,
                                  roots.hash(point(channel)), maxSegmentSize,
                                  replicator, appendSegmentCache,
                                  readSegmentCache);
        } else if (self.equals(pair.get(1))) {
            ec = new EventChannel(self, Role.MIRROR, pair.get(0), channel,
                                  roots.hash(point(channel)), maxSegmentSize,
                                  null, appendSegmentCache, readSegmentCache);
        } else {
            throw new IllegalStateException(
                                            String.format("%s is neither mirror nor primary for %, cannot xerox",
                                                          self, channel));
        }
        EventChannel previous = channels.put(channel, ec);
        assert previous == null : String.format("Xeroxed event channel %s is currently hosted on %s",
                                                channel, self);
        return ec;
    }

    private ConcurrentMap<File, Segment> createAppendSegmentCache(WeaverConfigation configuration,
                                                                  Builder<File, Segment> builder) {
        builder.initialCapacity(configuration.getInitialAppendSegmentCapacity());
        builder.concurrencyLevel(configuration.getAppendSegmentConcurrencyLevel());
        builder.maximumWeightedCapacity(configuration.getMaximumAppendSegmentCapacity());
        builder.listener(new EvictionListener<File, Segment>() {
            @Override
            public void onEviction(File file, Segment segment) {
                try {
                    segment.close();
                } catch (IOException e) {
                    log.trace(String.format("Error closing %s" + segment), e);
                }
                log.trace(String.format("%s evicted on %s", segment, self));
            }
        });
        return builder.build();
    }

    private ConcurrentMap<File, Segment> createReadSegmentCache(WeaverConfigation configuration,
                                                                Builder<File, Segment> builder) {
        builder.initialCapacity(configuration.getInitialReadSegmentCapacity());
        builder.concurrencyLevel(configuration.getReadSegmentConcurrencyLevel());
        builder.maximumWeightedCapacity(configuration.getMaximumReadSegmentCapacity());
        builder.listener(new EvictionListener<File, Segment>() {
            @Override
            public void onEviction(File file, Segment segment) {
                try {
                    segment.close();
                } catch (IOException e) {
                    log.trace(String.format("Error closing %s" + segment), e);
                }
                log.trace(String.format("%s evicted on %s", segment, self));
            }
        });
        return builder.build();
    }

    private void infoLog(String logString, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(String.format(logString, args));
        }
    }

    /**
     * Answer the remapped primary/mirror pairs using the nextRing
     * 
     * @param nextRing
     *            - the new weaver hash ring
     * @return the remapping of channels hosted on this node that have changed
     *         their primary or mirror in the new hash ring
     */
    protected Map<UUID, Node[][]> remap() {
        Map<UUID, Node[][]> remapped = new HashMap<UUID, Node[][]>();
        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            long channelPoint = point(entry.getKey());
            List<Node> newPair = nextRing.hash(channelPoint, 2);
            Node[] oldPair = entry.getValue().getOriginalMapping(self);
            if (oldPair[0].equals(self) || oldPair[1].equals(self)) {
                if (!oldPair[0].equals(newPair.get(0))
                    || !oldPair[1].equals(newPair.get(1))) {
                    remapped.put(entry.getKey(), new Node[][] {
                            entry.getValue().getOriginalMapping(self),
                            new Node[] { newPair.get(0), newPair.get(1) } });
                }
            }
        }
        return remapped;
    }

    protected void xeroxTo(EventChannel eventChannel, Node node,
                           Map<Node, Xerox> xeroxes,
                           WeaverCoordinator coordinator) {
        Xerox xerox = xeroxes.get(node);
        if (xerox == null) {
            xerox = new Xerox(self, node);
            xeroxes.put(node, xerox);
        }
        xerox.addChannel(eventChannel);
    }
}
