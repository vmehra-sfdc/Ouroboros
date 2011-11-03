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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.ChannelMessage;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.util.Association;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * The distributed coordinator for the channel buffer process group.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {

    private final static Logger                                 log             = Logger.getLogger(Coordinator.class.getCanonicalName());
    static final int                                            DEFAULT_TIMEOUT = 1;
    static final TimeUnit                                       TIMEOUT_UNIT    = TimeUnit.MINUTES;

    private final Set<UUID>                                     channels        = new HashSet<UUID>();
    private final Node                                          id;
    private final SortedSet<Node>                               members         = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>                               newMembers      = new ConcurrentSkipListSet<Node>();
    private final Switchboard                                   switchboard;
    private final ScheduledExecutorService                      timer;
    private final Weaver                                        weaver;
    private final AtomicReference<ConsistentHashFunction<Node>> weaverRing      = new AtomicReference<ConsistentHashFunction<Node>>(
                                                                                                                                    new ConsistentHashFunction<Node>());
    private final Map<Node, ContactInformation>                 yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();
    Rendezvous                                                  replicatorRendezvous;

    public Coordinator(ScheduledExecutorService timer, Switchboard switchboard,
                       Weaver weaver) {
        this.timer = timer;
        this.switchboard = switchboard;
        this.weaver = weaver;
        switchboard.setMember(this);
        weaver.setCoordinator(this);
        id = weaver.getId();
        yellowPages.put(id, weaver.getContactInformation());
    }

    @Override
    public void advertise() {
        ContactInformation info = yellowPages.get(id);
        switchboard.ringCast(new Message(
                                         weaver.getId(),
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
    public void close(UUID channel, Node requester) {
        channels.remove(channel);
        Node[] pair = getReplicationPair(channel);
        if (members.contains(pair[0])) {
            switchboard.send(new Message(id, ChannelMessage.CLOSE_MIRROR,
                                         new Association<Node, UUID>(requester,
                                                                     channel)),
                             pair[1]);
        } else {
            switchboard.send(new Message(id, ChannelMessage.CLOSED, channel),
                             requester);
        }
        weaver.close(channel);
    }

    /**
     * Close the channel if the node is a primary or mirror of the existing
     * channel.
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void closeMirror(UUID channel, Node requester) {
        channels.remove(channel);
        weaver.close(channel);
        switchboard.send(new Message(id, ChannelMessage.CLOSED, channel),
                         requester);
    }

    @Override
    public void destabilize() {
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable payload, long time) {
        switch (type) {
            case OPEN: {
                open((UUID) payload, sender);
                break;
            }
            case OPEN_MIRROR: {
                @SuppressWarnings("unchecked")
                Association<Node, UUID> ass = (Association<Node, UUID>) payload;
                openMirror(ass.value, ass.key);
                break;
            }
            case CLOSE: {
                close((UUID) payload, sender);
                break;
            }
            case CLOSE_MIRROR: {
                @SuppressWarnings("unchecked")
                Association<Node, UUID> ass = (Association<Node, UUID>) payload;
                closeMirror(ass.value, ass.key);
            }
            default: {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid target %s for channel message: %s",
                                              id, type));
                }
            }
        }
    }

    @Override
    public void dispatch(GlobalMessageType type, Node sender,
                         Serializable payload, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                members.add(sender);
                yellowPages.put(sender, (ContactInformation) payload);
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
            weaver.closeReplicator(node);
        }
        weaver.failover(deadMembers);
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
        List<Node> pair = weaverRing.get().hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    public boolean hasNewMembers() {
        return !newMembers.isEmpty();
    }

    /**
     * Open the new channel if this node is a primary or mirror of the new
     * channel.
     * 
     * @param channel
     *            - the id of the channel to open
     */
    public void open(UUID channel, Node requester) {
        Node[] pair = getReplicationPair(channel);
        if (!channels.add(channel)) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Channel is already opened %s", channel));
            }
            return;
        } else if (members.contains(pair[1])) {
            switchboard.send(new Message(id, ChannelMessage.OPEN_MIRROR,
                                         new Association<Node, UUID>(requester,
                                                                     channel)),
                             pair[1]);
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Opening primary for channel %s on mirror %s, primary %s is dead",
                                       channel, id, pair[0]));
            }
            switchboard.send(new Message(id, ChannelMessage.OPENED, channel),
                             requester);
        }
        weaver.openPrimary(channel, pair[1]);
    }

    /**
     * Open the new channel if this node is a primary or mirror of the new
     * channel.
     * 
     * @param channel
     *            - the id of the channel to open
     */
    public void openMirror(UUID channel, Node requester) {
        if (!channels.add(channel)) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Channel is already opened on mirror %s",
                                        channel));
            }
        } else {
            Node[] pair = getReplicationPair(channel);
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Opening mirror for channel %s on %s",
                                       channel, id));
            }
            weaver.openMirror(channel, pair[0]);
        }
        switchboard.send(new Message(id, ChannelMessage.OPENED, channel),
                         requester);
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
            weaver.openReplicator(member, yellowPages.get(member), rendezvous);
        }
        rendezvous.scheduleCancellation(DEFAULT_TIMEOUT, TIMEOUT_UNIT, timer,
                                        timeoutAction);
        return rendezvous;
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
            xeroxes.addAll(weaver.rebalance(entry.getKey(), entry.getValue(),
                                            deadMembers));
        }
        return xeroxes;
    }

    public void recoverPreviousReplicators() {
        // TODO Auto-generated method stub

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
            List<Node> oldPair = weaverRing.get().hash(channelPoint, 2);
            if (oldPair.contains(id)) {
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
    public void stabilized() {
        failover(switchboard.getDeadMembers());
        filterSystemMembership();
        Runnable action = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicators synchronization achieved on %s",
                                           id));
                }
            }
        };
        Runnable timeoutAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicators synchronization timed out on %s",
                                           id));
                }
            }
        };
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Synchronization of replicators initiated on %s",
                                   id));
        }
        replicatorRendezvous = openReplicators(newMembers, action,
                                               timeoutAction);
    }

    /**
     * Update the consistent hash function for the weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    public void updateRing(ConsistentHashFunction<Node> ring) {
        weaverRing.set(ring);
    }

    private void filterSystemMembership() {
        members.removeAll(switchboard.getDeadMembers());
        newMembers.clear();
        for (Node node : switchboard.getNewMembers()) {
            if (members.remove(node)) {
                newMembers.add(node);
            }
        }
    }
}
