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
import com.salesforce.ouroboros.spindle.messages.ReplicatorMessage;
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
    static final int                            DEFAULT_TIMEOUT = 1;
    static final TimeUnit                       TIMEOUT_UNIT    = TimeUnit.MINUTES;
    private final static Logger                 log             = Logger.getLogger(Coordinator.class.getCanonicalName());

    private final Set<UUID>                     channels        = new HashSet<UUID>();
    private final CoordinatorContext            fsm             = new CoordinatorContext(
                                                                                         this);
    private final Node                          id;
    private final SortedSet<Node>               members         = new ConcurrentSkipListSet<Node>();
    private final Collection<Node>              newMembers      = new ArrayList<Node>();
    private ConsistentHashFunction<Node>        nextRing;
    private final Switchboard                   switchboard;
    private final Collection<Node>              tally           = new ConcurrentSkipListSet<Node>();
    private final ScheduledExecutorService      timer;
    private final Weaver                        weaver;
    private ConsistentHashFunction<Node>        weaverRing      = new ConsistentHashFunction<Node>();
    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();
    private int                                 targetTally;

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
        fsm.destabilize();
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
     * Initiate the rebalancing of the weaver ring.
     */
    public void initiateRebalance() {
        fsm.rebalance();
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
     * @param controller
     *            - the controller for the group
     * 
     * @return - the Rendezvous used to synchronize with replication connections
     */
    public Rendezvous openReplicators(final Node controller) {
        Runnable rendezvousAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicators established on %s", id));
                }
                if (!isLeader()) {
                    fsm.replicatorsEstablished();
                }
                switchboard.send(new Message(
                                             id,
                                             ReplicatorMessage.REPLICATORS_ESTABLISHED),
                                 controller);
            }
        };
        if (newMembers.isEmpty()) {
            rendezvousAction.run();
            return null;
        }
        Runnable timeoutAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicator establishment timed out on %s",
                                           id));
                }
                fsm.replicatorsEstablishmentTimeout();
            }
        };
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Establishment of replicators initiated on %s",
                                   id));
        }
        Rendezvous rendezvous = new Rendezvous(newMembers.size(),
                                               rendezvousAction);
        for (Node member : newMembers) {
            weaver.openReplicator(member, yellowPages.get(member), rendezvous);
        }
        rendezvous.scheduleCancellation(DEFAULT_TIMEOUT, TIMEOUT_UNIT, timer,
                                        timeoutAction);
        return rendezvous;
    }

    public void replicatorsEstablished(Node sender) {
        assert isLeader() : "Replicator established method must only be sent to the group leader";
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Replicators reported established on: %s",
                                   sender));
        }
        tally.add(sender);
        fsm.replicatorsEstablished();
    }

    @Override
    public void stabilized() {
        fsm.stabilize();
    }

    /**
     * Calculate the next ring based on the current member set and the new
     * member set
     */
    protected void calculateNextRing() {
        ConsistentHashFunction<Node> newRing = new ConsistentHashFunction<Node>();
        for (Node node : members) {
            newRing.add(node, node.capacity);
        }
        for (Node node : newMembers) {
            newRing.add(node, node.capacity);
        }
        nextRing = newRing;
    }

    protected void cleanUpPendingReplicators() {
        // TODO Auto-generated method stub

    }

    protected void cleanUpRebalancing() {
        // TODO Auto-generated method stub

    }

    protected void closeDeadReplicators() {
        for (Node node : switchboard.getDeadMembers()) {
            weaver.closeReplicator(node);
        }
    }

    /**
     * Commit the calculated next ring as the current weaver ring
     */
    protected void commitNextRing() {
        assert nextRing != null : "Next ring has not been calculated";
        weaverRing = nextRing;
        nextRing = null;
    }

    /**
     * The
     */
    protected void coordinateRebalance() {
        assert isLeader() : "Must be leader to coordinate the rebalance";
        // TODO Auto-generated method stub
        tally.clear();
    }

    /**
     * The receiver is the coordinator for the group. Coordinate the
     * establishment of the replicators on the members and new members of the
     * group
     */
    protected void coordinateReplicators() {
        assert isLeader() : "Must be leader to coordinate the establishment of replicators";
        tally.clear();
        targetTally = members.size() + newMembers.size();
        Message message = new Message(id, ReplicatorMessage.OPEN_REPLICATORS);
        switchboard.broadcast(message, members);
        switchboard.broadcast(message, newMembers);
    }

    protected void coordinateTakeover() {
        assert isLeader() : "Must be leader to coordinate the takeover";
        // TODO Auto-generated method stub
        tally.clear();
    }

    /**
     * A fatal state exception occurred, so destabilize the partition
     */
    protected void destabilizePartition() {
        switchboard.destabilize();
    }

    /**
     * The weaver membership has partitioned. Failover any channels the dead
     * members were serving as primary that the receiver's node is providing
     * mirrors for.
     */
    protected void failover() {
        Collection<Node> deadMembers = switchboard.getDeadMembers();
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

    protected void filterSystemMembership() {
        members.removeAll(switchboard.getDeadMembers());
        newMembers.removeAll(switchboard.getDeadMembers());
        for (Node node : switchboard.getNewMembers()) {
            if (members.remove(node)) {
                newMembers.add(node);
            }
        }
    }

    /**
     * Answer true if the receiver is the leader of the group
     * 
     * @return
     */
    protected boolean isLeader() {
        return id.compareTo(members.last()) >= 0;
    }

    protected void rebalance() {
        calculateNextRing();
        rebalance(remap(), switchboard.getDeadMembers());
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
    protected List<Xerox> rebalance(Map<UUID, Node[][]> remapped,
                                    Collection<Node> deadMembers) {
        Runnable rendezvousAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weavers rebalanced on %s", id));
                }
                fsm.rebalanced();
            }
        };
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Establishment of replicators initiated on %s",
                                   id));
        }
        new Rendezvous(newMembers.size(), rendezvousAction);
        List<Xerox> xeroxes = new ArrayList<Xerox>();
        for (Entry<UUID, Node[][]> entry : remapped.entrySet()) {
            xeroxes.addAll(weaver.rebalance(entry.getKey(),
                                            entry.getValue()[0],
                                            entry.getValue()[1], deadMembers));
        }
        return xeroxes;
    }

    /**
     * Answer the remapped primary/mirror pairs using the nextRing
     * 
     * @return the remapping of channels hosted on this node that have changed
     *         their primary or mirror in the new hash ring
     */
    protected Map<UUID, Node[][]> remap() {
        Map<UUID, Node[][]> remapped = new HashMap<UUID, Node[][]>();
        for (UUID channel : channels) {
            long channelPoint = point(channel);
            List<Node> newPair = nextRing.hash(channelPoint, 2);
            List<Node> oldPair = weaverRing.hash(channelPoint, 2);
            if (oldPair.contains(id)) {
                if (!oldPair.get(0).equals(newPair.get(0))
                    || !oldPair.get(1).equals(newPair.get(1))) {
                    remapped.put(channel,
                                 new Node[][] {
                                         new Node[] { oldPair.get(0),
                                                 oldPair.get(1) },
                                         new Node[] { newPair.get(0),
                                                 newPair.get(1) } });
                }
            }
        }
        return remapped;
    }

    protected void revertRebalancing() {
        // TODO Auto-generated method stub

    }

    protected void switchProducers() {
        // TODO Auto-generated method stub

    }

    /**
     * @return true if the tally is equal to the required size
     */
    protected boolean tallyComplete() {
        return tally.size() == targetTally;
    }

    /**
     * Test access
     * 
     * @return the list of new members
     */
    Collection<Node> getNewMembers() {
        return newMembers;
    }

    /**
     * Set the consistent hash function for the next weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    void setNextRing(ConsistentHashFunction<Node> ring) {
        nextRing = ring;
    }
}
