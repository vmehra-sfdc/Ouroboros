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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the channel buffer process group.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator {

    private final static Logger log = Logger.getLogger(Coordinator.class.getCanonicalName());

    public static long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
    }

    private final Set<UUID>                     channels    = new HashSet<UUID>();
    private Weaver                              localWeaver;
    private final Map<Integer, Node>            members     = new HashMap<Integer, Node>();
    private ConsistentHashFunction<Node>        weaverRing  = new ConsistentHashFunction<Node>();
    private final Map<Node, ContactInformation> yellowPages = new HashMap<Node, ContactInformation>();

    /**
     * Add new members of the weaver process group.
     * 
     * @param newMembers
     *            - the new members
     * @return the new consistent hash ring
     */
    public ConsistentHashFunction<Node> addNewMembers(Collection<Node> newMembers) {
        ConsistentHashFunction<Node> newRing = weaverRing.clone();
        for (Node node : newMembers) {
            members.put(node.processId, node);
            newRing.add(node, node.capacity);
        }

        return newRing;
    }

    public CountDownLatch openReplicators(Map<Node, ContactInformation> newMembers) {
        CountDownLatch latch = new CountDownLatch(newMembers.size());
        yellowPages.putAll(newMembers);
        for (Entry<Node, ContactInformation> entry : newMembers.entrySet()) {
            localWeaver.openReplicator(entry.getKey(), entry.getValue(), latch);
        }
        return latch;
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
            members.remove(node.processId);
            yellowPages.remove(node);
            localWeaver.closeReplicator(node);
        }
        localWeaver.failover(deadMembers);
    }

    /**
     * Answer the contact information for the node.
     * 
     * @param node
     *            - the node
     * @return the ContactInformation for the node
     */
    public ContactInformation getContactInformationFor(Node node) {
        return yellowPages.get(node);
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
     * Set the local weaver
     * 
     * @param weaver
     *            - the local weaver for this coordinator
     */
    public void ready(Weaver weaver) {
        localWeaver = weaver;
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

    /**
     * Update the consistent hash function for the weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    public void updateRing(ConsistentHashFunction<Node> ring) {
        weaverRing = ring;
    }
}
