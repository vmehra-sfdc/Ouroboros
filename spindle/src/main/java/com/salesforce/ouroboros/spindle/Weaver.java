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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.partition.util.NodeIdSet;

import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
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

    public enum State {
        CREATED, INITIALIZED, RECOVERING, PARTITION, REBALANCING, RUNNING,
        REHASHING;
    }

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

    private static final Logger                          log           = Logger.getLogger(Weaver.class.getCanonicalName());

    private final Node                                   id;
    private final long                                   maxSegmentSize;
    private final ConcurrentMap<UUID, EventChannel>      channels      = new ConcurrentHashMap<UUID, EventChannel>();
    private final ConcurrentMap<Node, Replicator>        replicators   = new ConcurrentHashMap<Node, Replicator>();
    private final ServerSocketChannelHandler<Replicator> replicationHandler;
    private final File                                   root;
    private final ServerSocketChannelHandler<Spinner>    spindleHandler;
    private final AtomicReference<State>                 state         = new AtomicReference<Weaver.State>(
                                                                                                           State.CREATED);
    private final Set<UUID>                              subscriptions = new HashSet<UUID>();
    private final ConsistentHashFunction<Node>           weaverRing    = new ConsistentHashFunction<Node>();

    public Weaver(WeaverConfigation configuration) throws IOException {
        id = configuration.getId();
        weaverRing.add(id, 1);
        root = configuration.getRoot();
        maxSegmentSize = configuration.getMaxSegmentSize();
        replicationHandler = new ServerSocketChannelHandler<Replicator>(
                                                                        "Weaver Replicator",
                                                                        configuration.getReplicationSocketOptions(),
                                                                        configuration.getReplicationAddress(),
                                                                        Executors.newFixedThreadPool(2),
                                                                        new ReplicatorFactory());
        spindleHandler = new ServerSocketChannelHandler<Spinner>(
                                                                 "Weaver Spindle",
                                                                 configuration.getSpindleSocketOptions(),
                                                                 configuration.getSpindleAddress(),
                                                                 configuration.getSpindles(),
                                                                 new SpindleFactory());
        state.set(State.INITIALIZED);
    }

    public void close(UUID channel) {
        EventChannel eventChannel = channels.remove(channel);
        if (channel != null) {
            eventChannel.close();
        }
    }

    @Override
    public EventChannel eventChannelFor(EventHeader header) {
        UUID channelTag = header.getChannel();
        return channels.get(channelTag);
    }

    public InetSocketAddress getReplicatorEndpoint() {
        return replicationHandler.getLocalAddress();
    }

    public InetSocketAddress getSpindleEndpoint() {
        return spindleHandler.getLocalAddress();
    }

    public State getState() {
        return state.get();
    }

    /**
     * The system has successfully partitioned. Adjust tracking state of other
     * weavein
     * 
     * @param weavers
     *            - the mapping between the weaver's id and the socket address
     *            for this weaver's replication endpoint
     * @return true, if the partitioning was successful, false if an error
     *         occured during the adjustment to the partitioning event,
     *         requiring the partition to be disolved and reestablished
     */
    public boolean partition(Map<Node, InetSocketAddress> weavers) {
        state.set(State.PARTITION);

        // Clear out members that are not part of the partition
        clearDefunctMembers(weavers);
        // Add the new weavers in the partition
        if (!establishNewMembers(weavers)) {
            return false;
        }

        return true;
    }

    @Override
    public void registerReplicator(Node id, Replicator replicator) {
        replicators.put(id, replicator);
    }

    public void rehashChannels(NodeIdSet newMembers,
                               NodeIdSet survivingMembers, NodeIdSet deadMembers) {
        if (state.compareAndSet(State.PARTITION, State.REHASHING)) {
            String msg = String.format("Attempt to rehash when not in the PARTITION state: %s",
                                       state.get());
            log.severe(msg);
            throw new IllegalStateException(msg);
        }

        List<Xerox> copiers = new LinkedList<Xerox>();

        for (Entry<UUID, EventChannel> entry : channels.entrySet()) {
            final UUID channelId = entry.getKey();
            final EventChannel channel = entry.getValue();

            List<Node> pair = weaverRing.hash(point(channelId), 2);
            Xerox xerox;
            if (pair.get(0).equals(id)) {
                xerox = channel.makePrimary(pair.get(1), deadMembers);
            } else if (pair.get(1).equals(id)) {
                xerox = channel.makeSecondary(pair.get(0), deadMembers);
            } else {
                if (log.isLoggable(Level.FINE)) {
                    log.fine(String.format("Weaver[%s] is no longer the primary or secondary for: %s",
                                           id, channelId));
                }
                xerox = channel.transferControl(pair, deadMembers);
            }
            if (xerox != null) {
                copiers.add(xerox);
            }
        }

        for (UUID channel : subscriptions) {
            if (!channels.containsKey(channel)) {
                List<Node> pair = weaverRing.hash(point(channel), 2);
                if (pair.get(0).equals(id)) {
                    EventChannel ec = new EventChannel(
                                                       EventChannel.Role.PRIMARY,
                                                       channel,
                                                       root,
                                                       maxSegmentSize,
                                                       replicators.get(pair.get(1)).getDuplicator());
                    channels.put(channel, ec);
                } else if (pair.get(1).equals(id)) {
                    EventChannel ec = new EventChannel(
                                                       EventChannel.Role.MIRROR,
                                                       channel,
                                                       root,
                                                       maxSegmentSize,
                                                       replicators.get(pair.get(0)).getDuplicator());
                    channels.put(channel, ec);
                }
            }
        }
    }

    public void start() {
        spindleHandler.start();
        replicationHandler.start();
    }

    public void terminate() {
        spindleHandler.terminate();
        replicationHandler.terminate();
    }

    @Override
    public String toString() {
        return String.format("Weaver[%s], spindle endpoint: %s, replicator endpoint: %s",
                             id, getSpindleEndpoint(), getReplicatorEndpoint());
    }

    private void clearDefunctMembers(Map<Node, InetSocketAddress> weavers) {
        for (Entry<Node, Replicator> entry : replicators.entrySet()) {
            Node weaverId = entry.getKey();
            if (!weavers.containsKey(weaverId)) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weaver[%s] is no longer a member of the partition",
                                           weaverId));
                }
                entry.getValue().close();
                replicators.remove(weaverId);
                weaverRing.remove(weaverId);
            }
        }
    }

    private boolean establishNewMembers(Map<Node, InetSocketAddress> weavers) {
        for (Entry<Node, InetSocketAddress> entry : weavers.entrySet()) {
            Node weaverId = entry.getKey();
            if (!replicators.containsKey(weaverId)) {
                if (log.isLoggable(Level.INFO)) {
                    log.fine(String.format("Adding new weaver[%s] to the partition",
                                           weaverId));
                }
                weaverRing.add(weaverId, 1);
                Replicator replicator = new Replicator(weaverId, this);
                if (thisEndInitiatesConnectionsTo(weaverId)) {
                    if (log.isLoggable(Level.INFO)) {
                        log.fine(String.format("Initiating connection from weaver[%s] to new weaver[%s]",
                                               id, weaverId));
                    }
                    try {
                        replicationHandler.connectTo(entry.getValue(),
                                                     replicator);
                    } catch (IOException e) {
                        // We screwed.  Log this #fail and force a repartition event
                        log.log(Level.SEVERE,
                                String.format("Unable to connect to weaver: %s at replicator port: %s",
                                              entry.getKey(), entry.getValue()),
                                e);
                        return false;
                    }
                    replicators.put(weaverId, replicator);
                } else {
                    if (log.isLoggable(Level.INFO)) {
                        log.fine(String.format("Waiting for connection to weaver[%s] from new weaver[%s]",
                                               id, weaverId));
                    }
                }
            }
        }
        return true;
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
}
