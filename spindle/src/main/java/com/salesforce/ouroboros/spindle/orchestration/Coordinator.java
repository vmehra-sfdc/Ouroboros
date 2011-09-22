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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.Xerox;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * The distributed coordinator for the channel buffer process group.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {

    private final static Logger log             = Logger.getLogger(Coordinator.class.getCanonicalName());
    static final int            DEFAULT_TIMEOUT = 1;
    static final TimeUnit       TIMEOUT_UNIT    = TimeUnit.MINUTES;

    public static long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
    }

    private final Set<UUID>                               channels    = new HashSet<UUID>();
    private final AtomicReference<StateMachine>           current;
    private Weaver                                        localWeaver;
    private final SortedSet<Node>                         members     = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>                         newMembers  = new ConcurrentSkipListSet<Node>();
    private Switchboard                                   switchboard;
    private final ScheduledExecutorService                timer;
    private ConsistentHashFunction<Node>                  weaverRing  = new ConsistentHashFunction<Node>();
    private final ConcurrentMap<Node, ContactInformation> yellowPages = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(ScheduledExecutorService timer) {
        this.timer = timer;
        current = new AtomicReference<StateMachine>(
                                                    new ReplicationCoordinatorStateMachine(
                                                                                           this));
    }

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
        current.get().destabilize();
    }

    @Override
    public void dispatch(GlobalMessageType type, Node sender,
                         Serializable payload, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                members.add(sender);
                yellowPages.putIfAbsent(sender, (ContactInformation) payload);
                break;
            default:
                break;
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.MemberDispatch, com.salesforce.ouroboros.Node, java.io.Serializable, long)
     */
    @Override
    public void dispatch(MemberDispatch type, Node sender,
                         Serializable payload, long time) {
        current.get().transition((StateMachineDispatch) type, sender, payload,
                                 time);
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

    public Node getId() {
        return localWeaver.getId();
    }

    public SortedSet<Node> getMembers() {
        return members;
    }

    public SortedSet<Node> getNewMembers() {
        return newMembers;
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

    public Switchboard getSwitchboard() {
        return switchboard;
    }

    public ScheduledExecutorService getTimer() {
        return timer;
    }

    /**
     * @return true if this process is the leader of the group.
     */
    public boolean isLeader() {
        return localWeaver.getId().equals(members.first());
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
     * @param rendezvousAction
     *            - the action to execute when the rendezvous occurs
     * @param timeoutAction
     *            - the action to execute when the rendezvous times out
     * @return - the Rendezvous used to synchronize with replication connections
     */
    public Rendezvous openReplicators(Collection<Node> newMembers,
                                      Runnable rendezvousAction,
                                      Runnable timeoutAction) {
        Rendezvous rendezvous = new Rendezvous(newMembers.size(),
                                               rendezvousAction);
        for (Node member : newMembers) {
            localWeaver.openReplicator(member, yellowPages.get(member),
                                       rendezvous);
        }
        rendezvous.scheduleCancellation(DEFAULT_TIMEOUT, TIMEOUT_UNIT, timer,
                                        timeoutAction);
        return rendezvous;
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
        failover(switchboard.getDeadMembers());
        filterSystemMembership();
        current.get().stabilized();
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

    private void filterSystemMembership() {
        members.removeAll(switchboard.getDeadMembers());
        newMembers.clear();
        for (Node node : switchboard.getNewMembers()) {
            if (members.contains(node)) {
                newMembers.add(node);
            }
        }
    }
}
