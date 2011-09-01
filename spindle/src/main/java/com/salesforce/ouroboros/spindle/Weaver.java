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
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.locator.AnubisListener;
import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.locator.AnubisProvider;
import org.smartfrog.services.anubis.locator.AnubisStability;
import org.smartfrog.services.anubis.locator.AnubisValue;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

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
            return new Replicator(Weaver.this);
        }
    }

    private class SpindleFactory implements
            CommunicationsHandlerFactory<Spinner> {
        @Override
        public Spinner createCommunicationsHandler(SocketChannel channel) {
            return new Spinner(Weaver.this);
        }
    }

    /**
     * 
     * Provides the distributed orchestration for the weaver processes
     * 
     */
    class Orchestrator extends AnubisListener {
        private class Stability extends AnubisStability {
            @Override
            public void stability(boolean isStable, long timeRef) {
                if (isStable) {
                    partition();
                }
            }
        }

        private final AnubisLocator  locator;
        private final Stability      stability;
        private final AnubisProvider stateProvider;
        private final long           timeout;
        private final TimeUnit       unit;

        public Orchestrator(String stateName, AnubisLocator anubisLocator,
                            long timeout, TimeUnit unit) {
            super(stateName);
            this.timeout = timeout;
            this.unit = unit;
            locator = anubisLocator;
            stateProvider = new AnubisProvider(stateName);
            stability = new Stability();
            locator.registerStability(stability);
            locator.registerProvider(stateProvider);
            locator.registerListener(this);
        }

        @Override
        public void newValue(AnubisValue value) {
            if (value.getValue() == null) {
                return; // Initial value
            }
        }

        @Override
        public void removeValue(AnubisValue value) {
            // TODO Auto-generated method stub

        }

        private void partition() {
            Collection<Node> deadMembers = null;
            Map<Node, ContactInfomation> newMembers = null;
            ConsistentHashFunction<Node> previousRing = Weaver.this.partition(deadMembers,
                                                                              newMembers);
            rehashSubscriptions();
            CountDownLatch latch = rehashExistingChannels(previousRing);
            try {
                latch.await(timeout, unit);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static final Logger                          log           = Logger.getLogger(Weaver.class.getCanonicalName());

    private final ConcurrentMap<UUID, EventChannel>      channels      = new ConcurrentHashMap<UUID, EventChannel>();
    private final Node                                   id;
    private final long                                   maxSegmentSize;
    private final ServerSocketChannelHandler<Replicator> replicationHandler;
    private final ConcurrentMap<Node, Replicator>        replicators   = new ConcurrentHashMap<Node, Replicator>();
    private final File                                   root;
    private final ServerSocketChannelHandler<Spinner>    spindleHandler;
    private final Set<UUID>                              subscriptions = new HashSet<UUID>();
    private final ConsistentHashFunction<Node>           weaverRing    = new ConsistentHashFunction<Node>();
    private final ChannelHandler<Xerox>                  xeroxHandler;
    private final Map<Node, ContactInfomation>           yellowPages   = new HashMap<Node, ContactInfomation>();
    final Orchestrator                                   orchestrator;

    public Weaver(WeaverConfigation configuration, AnubisLocator locator)
                                                                         throws IOException {
        configuration.validate();
        orchestrator = new Orchestrator(configuration.getStateName(), locator,
                                        configuration.getPartitionTimeout(),
                                        configuration.getPartitionTimeoutUnit());
        id = configuration.getId();
        weaverRing.add(id, id.capacity);
        root = configuration.getRoot();
        maxSegmentSize = configuration.getMaxSegmentSize();
        xeroxHandler = new ChannelHandler<Xerox>(
                                                 "Weaver Xerox",
                                                 configuration.getXeroxSocketOptions(),
                                                 configuration.getXeroxes());
        replicationHandler = new ServerSocketChannelHandler<Replicator>(
                                                                        "Weaver Replicator",
                                                                        configuration.getReplicationSocketOptions(),
                                                                        configuration.getReplicationAddress(),
                                                                        configuration.getReplicators(),
                                                                        new ReplicatorFactory());
        spindleHandler = new ServerSocketChannelHandler<Spinner>(
                                                                 "Weaver Spindle",
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
    }

    public void close(UUID channel) {
        EventChannel eventChannel = channels.remove(channel);
        if (channel != null) {
            eventChannel.close();
        }
    }

    @Override
    public EventChannel eventChannelFor(EventHeader header) {
        return channels.get(header.getChannel());
    }

    @Override
    public void registerReplicator(Node id, Replicator replicator) {
        replicators.put(id, replicator);
    }

    public void start() {
        spindleHandler.start();
        replicationHandler.start();
        xeroxHandler.start();
    }

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

    /*
     * This weaver is the mirror of the channel in the new partition
     */
    private void partitionMirror(final UUID channelId,
                                 final EventChannel channel, List<Node> pair,
                                 LinkedList<Xerox> xeroxes) {
        switch (channel.getRole()) {
            case PRIMARY: {
                // The circle has a new primary, make this node the mirror and xerox the state
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("New primary for %s has been determined and is: %s",
                                           channelId, pair.get(1)));
                }
                channel.setMirror();
                xeroxes.add(new Xerox(pair.get(0), channel));
            }
            case MIRROR: {
                if (!channel.getReplicator().getId().equals(pair.get(0))) {
                    // The primary for this channel has died, xerox state to the new primary
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Mirror for %s has died, new mirror: %s",
                                               channelId, pair.get(1)));
                    }
                    xeroxes.add(new Xerox(pair.get(0), channel));
                }
            }
        }
    }

    /*
     * The new partition has created a new primary and mirror for the channel.
     */
    private void partitionNewPrimaryAndMirror(UUID channelId,
                                              EventChannel channel,
                                              List<Node> oldPair,
                                              List<Node> newPair,
                                              LinkedList<Xerox> xeroxes) {
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Weaver[%s] is no longer the primary or secondary for: %s",
                                   id, channelId));
        }
        switch (channel.getRole()) {
            case PRIMARY: {

            }
            case MIRROR: {

            }
        }
    }

    /*
     * This weaver is the primary of the channel in the new partition
     */
    private void partitionPrimary(final UUID channelId,
                                  final EventChannel channel, List<Node> pair,
                                  List<Xerox> xeroxes) {
        switch (channel.getRole()) {
            case PRIMARY: {
                if (!channel.getReplicator().getId().equals(pair.get(1))) {
                    // The mirror for this channel has died, xerox state to the new mirror
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Mirror for %s has died, new mirror: %s",
                                               channelId, pair.get(1)));
                    }
                    xeroxes.add(new Xerox(pair.get(1), channel));
                }
                break;
            }
            case MIRROR: {
                // This node is now the primary for the channel, xerox state to the new mirror
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weaver[%s] assuming primary role for: %s, mirror is %s",
                                           id, channelId, pair.get(1)));
                }
                channel.setPrimary();
                xeroxes.add(new Xerox(pair.get(1), channel));
            }
                break;
        }
    }

    private long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
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

    void addSubscription(UUID channel) {
        List<Node> pair = weaverRing.hash(point(channel), 2);
        if (pair.get(0).equals(id)) {
            // This node is the primary for the event channel
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format(" Weaver[%s] is the primary for the new subscription %s",
                                       id, channel));
            }
            EventChannel ec = new EventChannel(Role.PRIMARY, channel, root,
                                               maxSegmentSize,
                                               replicators.get(pair.get(1)));
            channels.put(channel, ec);
        } else if (pair.get(1).equals(id)) {
            // This node is the mirror for the event channel
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format(" Weaver[%s] is the mirror for the new subscription %s",
                                       id, channel));
            }
            EventChannel ec = new EventChannel(Role.MIRROR, channel, root,
                                               maxSegmentSize,
                                               replicators.get(pair.get(1)));
            channels.put(channel, ec);
        }
    }

    /**
     * The weaver membership has partitioned. Clear out the dead members,
     * accomidate the new members and normalize the state
     * 
     * @param deadMembers
     *            - the weaver nodes that have failed and are no longer part of
     *            the partition
     * @param newMembers
     *            - the new weaver nodes that are now part of the partition
     * @return the previous consistent hash ring for this weaver
     */
    ConsistentHashFunction<Node> partition(Collection<Node> deadMembers,
                                           Map<Node, ContactInfomation> newMembers) {
        ConsistentHashFunction<Node> previousRing = weaverRing.clone();
        for (Node node : deadMembers) {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Removing weaver[%s] from the partition",
                                       node));
            }
            yellowPages.remove(node);
            Replicator replicator = replicators.remove(node);
            if (replicators != null) {
                replicator.close();
            }
        }
        yellowPages.putAll(newMembers);
        for (Entry<Node, ContactInfomation> entry : newMembers.entrySet()) {
            Node node = entry.getKey();
            weaverRing.add(node, node.capacity);
            Replicator replicator = new Replicator(node, this);
            if (thisEndInitiatesConnectionsTo(node)) {
                if (log.isLoggable(Level.INFO)) {
                    log.fine(String.format("Initiating connection from weaver[%s] to new weaver[%s]",
                                           id, node));
                }
                try {
                    replicationHandler.connectTo(entry.getValue().replication,
                                                 replicator);
                } catch (IOException e) {
                    // We be screwed.  Log this #fail and force a repartition event
                    String msg = String.format("Unable to connect to weaver: %s at replicator port: %s",
                                               entry.getKey(), entry.getValue());
                    log.log(Level.SEVERE, msg, e);
                    throw new IllegalStateException(msg, e);
                }
                replicators.put(node, replicator);
            } else {
                if (log.isLoggable(Level.INFO)) {
                    log.fine(String.format("Waiting for connection to weaver[%s] from new weaver[%s]",
                                           id, node));
                }
            }
        }
        return previousRing;
    }

    /**
     * Rehash the existing channels that are currently maintained (either
     * primary or mirror) on the recevier
     * 
     * @param previousRing
     *            the previous weaver ring mapping
     * @return the CountDownLatch for synchronizing with any state transfer
     */
    CountDownLatch rehashExistingChannels(ConsistentHashFunction<Node> previousRing) {
        LinkedList<Xerox> xeroxes = new LinkedList<Xerox>();
        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            final UUID channelId = entry.getKey();
            final EventChannel channel = entry.getValue();
            long channelPoint = point(channelId);
            List<Node> pair = weaverRing.hash(channelPoint, 2);
            if (pair.get(0).equals(id)) {
                // This node is the primary for the channel
                partitionPrimary(channelId, channel, pair, xeroxes);
            } else if (pair.get(1).equals(id)) {
                // This node is the mirror for the channel
                partitionMirror(channelId, channel, pair, xeroxes);
            } else {
                // The new partition has a new primary and mirror for the channel
                partitionNewPrimaryAndMirror(channelId,
                                             channel,
                                             previousRing.hash(channelPoint, 2),
                                             pair, xeroxes);
            }
        }
        CountDownLatch latch = new CountDownLatch(xeroxes.size());
        for (Xerox xerox : xeroxes) {
            xerox.setLatch(latch);
            InetSocketAddress endpoint = yellowPages.get(xerox.getNode()).xerox;
            try {
                xeroxHandler.connectTo(endpoint, xerox);
            } catch (IOException e) {
                String msg = String.format("Cannot connect from: weaver[%s] to xerox on weaver[%s], endpoint: %s",
                                           id, xerox.getNode(), endpoint);
                log.log(Level.SEVERE, msg, e);
                throw new IllegalStateException(msg, e);
            }
        }
        return latch;
    }

    /**
     * Rehash the subscription channel distribution on the weaver partition. Do
     * not rehash any subscriptions that are already handled by the receiver, as
     * these will be handled in
     * {@link #rehashExistingChannels(ConsistentHashFunction)}
     */
    void rehashSubscriptions() {
        for (UUID channel : subscriptions) {
            if (!channels.containsKey(channel)) {
                List<Node> pair = weaverRing.hash(point(channel), 2);
                if (pair.get(0).equals(id)) {
                    if (log.isLoggable(Level.INFO)) {
                        log.fine(String.format("Weaver[%s] is now the primary for: %s, waiting for xerox",
                                               id, channel));
                    }
                    EventChannel ec = new EventChannel(
                                                       Role.PRIMARY,
                                                       channel,
                                                       root,
                                                       maxSegmentSize,
                                                       replicators.get(pair.get(1)));
                    channels.put(channel, ec);
                } else if (pair.get(1).equals(id)) {
                    if (log.isLoggable(Level.INFO)) {
                        log.fine(String.format("Weaver[%s] is now the secondary for: %s, waiting for xerox",
                                               id, channel));
                    }
                    EventChannel ec = new EventChannel(
                                                       Role.MIRROR,
                                                       channel,
                                                       root,
                                                       maxSegmentSize,
                                                       replicators.get(pair.get(0)));
                    channels.put(channel, ec);
                }
            }
        }
    }
}
