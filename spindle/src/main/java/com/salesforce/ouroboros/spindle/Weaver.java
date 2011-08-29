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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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
        CREATED, INITIALIZED, RECOVERING, PARTITION_CHANGE, RUNNING;
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

    private final int                                    id;
    private final long                                   maxSegmentSize;
    private final ConcurrentMap<UUID, EventChannel>      openChannels  = new ConcurrentHashMap<UUID, EventChannel>();
    private final ConcurrentMap<Integer, Replicator>     replicators   = new ConcurrentHashMap<Integer, Replicator>();
    private final ServerSocketChannelHandler<Replicator> replicationHandler;
    private final File                                   root;
    private final ServerSocketChannelHandler<Spinner>    spindleHandler;
    private final ConcurrentMap<UUID, Object>            subscriptions = new ConcurrentHashMap<UUID, Object>();
    private final AtomicReference<State>                 state         = new AtomicReference<Weaver.State>(
                                                                                                           State.CREATED);
    private final ConsistentHashFunction<Integer>        weaverRing    = new ConsistentHashFunction<Integer>();

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
        EventChannel eventChannel = openChannels.remove(channel);
        if (channel != null) {
            eventChannel.close();
        }
    }

    @Override
    public EventChannel eventChannelFor(EventHeader header) {
        UUID channelTag = header.getChannel();
        return openChannels.get(channelTag);
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

    public void open(UUID channel) {
        openChannels.putIfAbsent(channel,
                                 new EventChannel(channel, root,
                                                  maxSegmentSize, null));
    }

    /**
     * The partition has changed.
     * 
     * @param weavers
     *            - the mapping between the weaver's id and the socket address
     *            for this weaver's replication endpoint
     */
    public void partitionChange(Map<Integer, InetSocketAddress> weavers) {
        state.set(State.PARTITION_CHANGE);

        // Clear out members that are not part of the partition
        for (Map.Entry<Integer, Replicator> entry : replicators.entrySet()) {
            Integer weaverId = entry.getKey();
            if (!weavers.containsKey(weaverId)) {
                entry.getValue().close();
                replicators.remove(weaverId);
                weaverRing.remove(weaverId);
            }
        }
        // Add the new weavers in the partition
        for (Map.Entry<Integer, InetSocketAddress> entry : weavers.entrySet()) {
            Integer weaverId = entry.getKey();
            if (!replicators.containsKey(weaverId)) {
                weaverRing.add(weaverId, 1);
                if (thisEndInitiatesConnectionsTo(weaverId)) {

                }
            }
        }
    }

    /**
     * indicates which end initiates a connection to a given node
     * 
     * @param target
     *            - the other end of the intended connection
     * @return - true if this end initiates the connection, false otherwise
     */
    private boolean thisEndInitiatesConnectionsTo(int target) {
        return id < target;
    }

    @Override
    public void registerReplicator(int id, Replicator replicator) {
        replicators.put(id, replicator);
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
}
