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
package com.salesforce.ouroboros.spindle.orchestration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.Xerox;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the channel buffer process group.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {
    public enum Transition {
        DESTABILIZED, STABILIZED, SYNCHRONIZED;
    };

    public enum State {
        STABLIZED {
            @Override
            public void transition(Coordinator coordinator, Transition t) {
                switch (t) {
                    case DESTABILIZED: {
                    }
                }
            }
        },
        SYNCHRONIZED {
            @Override
            public void transition(Coordinator coordinator, Transition t) {
                // TODO Auto-generated method stub

            }
        },
        REBALANCED {
            @Override
            public void transition(Coordinator coordinator, Transition t) {
                // TODO Auto-generated method stub

            }
        };

        public abstract void transition(Coordinator coordinator, Transition t);
    }

    private final static Logger log = Logger.getLogger(Coordinator.class.getCanonicalName());

    public static long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
    }

    private final Set<UUID>                               channels    = new HashSet<UUID>();
    private Weaver                                        localWeaver;
    private final SortedSet<Node>                         newMembers  = new TreeSet<Node>();
    private volatile CyclicBarrier                        replicatorBarrier;
    private final SortedSet<Node>                         spindles    = new TreeSet<Node>();
    private Switchboard                                   switchboard;
    private ConsistentHashFunction<Node>                  weaverRing  = new ConsistentHashFunction<Node>();
    private final ConcurrentMap<Node, ContactInformation> yellowPages = new ConcurrentHashMap<Node, ContactInformation>();

    @Override
    public void advertise() {
        assert localWeaver != null : "local weaver has not been initialized";
        ContactInformation info = yellowPages.get(localWeaver.getId());
        assert info != null : "local weaver contact information has not been initialized";
        switchboard.ringCast(new Message(
                                         localWeaver.getId(),
                                         GlobalMessageType.ADVERTISE_CHANNEL_BUFFER,
                                         info));
    }

    /**
     * Close the channel if the node is a primary or mirror of the existing
     * channel.
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        channels.remove(channel);
        List<Node> pair = weaverRing.hash(point(channel), 2);
        if (pair != null) {
            if (pair.get(0).equals(localWeaver.getId())
                || pair.get(1).equals(localWeaver.getId())) {
                localWeaver.close(channel);
            }
        }
    }

    @Override
    public void destabilize() {
        replicatorBarrier.reset();
    }

    @Override
    public void discoverChannelBuffer(Node node, ContactInformation info,
                                      long time) {
        spindles.add(node);
        yellowPages.putIfAbsent(node, info);
    }

    @Override
    public void discoverConsumer(Node sender, long time) {
        // do nothing
    }

    @Override
    public void discoverProducer(Node sender, long time) {
        // do nothing
    }

    /**
     * The weaver membership has partitioned. Failover any channels the dead
     * members were serving as primary that the receiver's node is providing
     * mirrors for
     * 
     * @param deadMembers
     *            - the weaver nodes that have failed and are no longer part of
     *            the partition
     */
    public void failover(Collection<Node> deadMembers) {
        for (Node node : deadMembers) {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Removing weaver[%s] from the partition",
                                       node));
            }
            yellowPages.remove(node);
            localWeaver.closeReplicator(node);
        }
        localWeaver.failover(deadMembers);
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

    /**
     * Open the new channel if this node is a primary or mirror of the new
     * channel.
     * 
     * @param channel
     *            - the id of the channel to open
     */
    public void open(UUID channel) {
        channels.add(channel);
        List<Node> pair = weaverRing.hash(point(channel), 2);
        if (pair.get(0).equals(localWeaver.getId())) {
            localWeaver.openPrimary(channel, pair.get(1));
        } else if (pair.get(1).equals(localWeaver.getId())) {
            localWeaver.openMirror(channel, pair.get(0));
        }
    }

    /**
     * Open the replicators to the the new members
     * 
     * @param newMembers
     *            - the new member nodes
     * @param barrierAction
     *            - the action to execute when the new replicators are active
     * @return - the CyclicBarrier used to synchronize with replication
     *         connections
     */
    public CyclicBarrier openReplicators(Collection<Node> newMembers,
                                         Runnable barrierAction) {
        CyclicBarrier barrier = new CyclicBarrier(newMembers.size(),
                                                  barrierAction);
        for (Node member : newMembers) {
            localWeaver.openReplicator(member, yellowPages.get(member), barrier);
        }
        return barrier;
    }

    /**
     * Set the local weaver
     * 
     * @param weaver
     *            - the local weaver for this coordinator
     */
    public void ready(Weaver weaver, ContactInformation info) {
        localWeaver = weaver;
        yellowPages.put(weaver.getId(), info);
    }

    /**
     * Rebalance the channels which this node has responsibility for
     * 
     * @param remapped
     *            - the mapping of channels to their new primary/mirror pairs
     * @param deadMembers
     *            - the weaver nodes that have failed and are no longer part of
     *            the partition
     */
    public List<Xerox> rebalance(Map<UUID, Node[]> remapped,
                                 Collection<Node> deadMembers) {
        List<Xerox> xeroxes = new ArrayList<Xerox>();
        for (Entry<UUID, Node[]> entry : remapped.entrySet()) {
            xeroxes.addAll(localWeaver.rebalance(entry.getKey(),
                                                 entry.getValue(), deadMembers));
        }
        return xeroxes;
    }

    /**
     * Answer the remapped primary/mirror pairs given the new ring
     * 
     * @param newRing
     *            - the new consistent hash ring of nodes
     * @return the remapping of channels hosted on this node that have changed
     *         their primary or mirror in the new hash ring
     */
    public Map<UUID, Node[]> remap(ConsistentHashFunction<Node> newRing) {
        Map<UUID, Node[]> remapped = new HashMap<UUID, Node[]>();
        for (UUID channel : channels) {
            long channelPoint = point(channel);
            List<Node> newPair = newRing.hash(channelPoint, 2);
            List<Node> oldPair = weaverRing.hash(channelPoint, 2);
            if (oldPair.contains(localWeaver.getId())) {
                if (!oldPair.get(0).equals(newPair.get(0))
                    || !oldPair.get(1).equals(newPair.get(1))) {
                    remapped.put(channel,
                                 new Node[] { newPair.get(0), newPair.get(1) });
                }
            }
        }
        return remapped;
    }

    @Override
    public void setSwitchboard(Switchboard switchboard) {
        this.switchboard = switchboard;
    }

    @Override
    public void stabilized() {
        // Failover
        failover(switchboard.getDeadMembers());

        // Filter the system membership
        spindles.removeAll(switchboard.getDeadMembers());
        newMembers.clear();
        for (Node node : switchboard.getNewMembers()) {
            if (spindles.contains(node)) {
                newMembers.add(node);
            }
        }
        Runnable barrierAction = new Runnable() {
            @Override
            public void run() {
                switchboard.send(new Message(
                                             localWeaver.getId(),
                                             SpindleMessage.REPLICATORS_SYNCHRONIZED),
                                 spindles.first());
            }
        };
        replicatorBarrier = openReplicators(newMembers, barrierAction);
    }

    /**
     * Update the consistent hash function for the weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    public void updateRing(ConsistentHashFunction<Node> ring) {
        weaverRing = ring;
    }

    public void replicatorsSynchronizedOn(Node sender) {
        // TODO Auto-generated method stub

    }
}
