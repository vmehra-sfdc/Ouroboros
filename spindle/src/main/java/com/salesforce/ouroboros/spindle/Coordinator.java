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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import statemap.StateUndefinedException;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.RebalanceMessage;
import com.salesforce.ouroboros.spindle.CoordinatorContext.CoordinatorState;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.replication.ReplicatorContext.ReplicatorFSM;
import com.salesforce.ouroboros.spindle.transfer.Xerox;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * The distributed coordinator for the channel buffer process group.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {

    private final static Logger                 log             = Logger.getLogger(Coordinator.class.getCanonicalName());
    static final int                            DEFAULT_TIMEOUT = 5;
    static final TimeUnit                       TIMEOUT_UNIT    = TimeUnit.MINUTES;

    private boolean                             active          = false;
    private final SortedSet<Node>               activeMembers   = new ConcurrentSkipListSet<Node>();
    private final CoordinatorContext            fsm             = new CoordinatorContext(
                                                                                         this);
    private final SortedSet<Node>               inactiveMembers = new ConcurrentSkipListSet<Node>();
    private Node[]                              joiningMembers  = new Node[0];
    private final SortedSet<Node>               nextMembership  = new ConcurrentSkipListSet<Node>();
    private Rendezvous                          rendezvous;
    private final Node                          self;
    private final Switchboard                   switchboard;
    private final AtomicInteger                 tally           = new AtomicInteger();
    private final ScheduledExecutorService      timer;
    private final Weaver                        weaver;
    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(ScheduledExecutorService timer, Switchboard switchboard,
                       Weaver weaver) throws IOException {
        this.timer = timer;
        this.switchboard = switchboard;
        this.weaver = weaver;
        switchboard.setMember(this);
        self = weaver.getId();
        fsm.setName(Integer.toString(self.processId));
        yellowPages.put(self, weaver.getContactInformation());
    }

    @Override
    public void advertise() {
        ContactInformation info = yellowPages.get(self);
        switchboard.ringCast(new Message(
                                         weaver.getId(),
                                         DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER,
                                         info, active));
    }

    @Override
    public void becomeInactive() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Spindle %s is now inactivated", self));
        }
        active = false;
        weaver.inactivate();
        cleanUp();
    }

    /**
     * Close the channel if the node is a primary or mirror of the existing
     * channel.
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        weaver.close(channel);
    }

    @Override
    public void destabilize() {
        fsm.destabilize();
    }

    @Override
    public void dispatch(BootstrapMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case BOOTSTAP_PRODUCERS:
                break;
            case BOOTSTRAP_SPINDLES:
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Bootstrapping spindles on %s", self));
                }
                bootstrap((Node[]) arguments[0]);
                fsm.bootstrapped();
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Illegal bootstrap method: %s",
                                                              type));
        }
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case OPEN: {
                open((UUID) arguments[0]);
                break;
            }
            case CLOSE: {
                close((UUID) arguments[0]);
                break;
            }
            case PRIMARY_OPENED:
                break;
            case MIRROR_OPENED:
                break;
            default: {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid target %s for channel message: %s",
                                              self, type));
                }
            }
        }
    }

    @Override
    public void dispatch(DiscoveryMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                if ((Boolean) arguments[1]) {
                    activeMembers.add(sender);
                } else {
                    inactiveMembers.add(sender);
                }
                yellowPages.put(sender, (ContactInformation) arguments[0]);
                break;
            default:
                break;
        }
    }

    @Override
    public void dispatch(FailoverMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PREPARE:
                failover();
                break;
            case FAILOVER:
                // do nothing
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Unknown failover message: %s",
                                                              type));
        }
    }

    public void dispatch(RebalancedMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case MEMBER_REBALANCED: {
                if (isActiveLeader()) {
                    log.info(String.format("%s marked as rebalanced on %s",
                                           sender, self));
                    tally.decrementAndGet();
                    fsm.memberRebalanced();
                } else {
                    switchboard.forwardToNextInRing(new Message(sender, type,
                                                                arguments),
                                                    nextMembership);
                }
                break;
            }
            default:
                throw new IllegalStateException(
                                                String.format("Invalid rebalanced message: %s",
                                                              type));
        }
    }

    @Override
    public void dispatch(RebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PREPARE_FOR_REBALANCE:
                rebalance((Node[]) arguments[0]);
                break;
            case INITIATE_REBALANCE:
                rebalance();
                break;
            case TAKEOVER:
                rebalanced();
                break;
            case REBALANCE_COMPLETE:
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid rebalance message %s",
                                                              type));
        }
    }

    public void dispatch(ReplicatorMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case BEGIN_REBALANCE:
                fsm.beginRebalance();
                break;
            case ESTABLISH_REPLICATORS:
                establishReplicators();
                break;
            case REPLICATORS_ESTABLISHED:
                if (isActiveLeader()) {
                    replicatorsEstablished(sender);
                } else {
                    switchboard.forwardToNextInRing(new Message(sender, type,
                                                                arguments),
                                                    nextMembership);
                }
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid replicator message: %s",
                                                              type));
        }
    }

    public Node getId() {
        return self;
    }

    /**
     * @return the state of the reciver. return null if the state is undefined,
     *         such as when the coordinator is transititioning between states
     */
    public CoordinatorState getState() {
        try {
            return fsm.getState();
        } catch (StateUndefinedException e) {
            return null;
        }
    }

    /**
     * Initiate the bootstrapping of the weaver ring using the set of inactive
     * members
     */
    public void initiateBootstrap() {
        initiateBootstrap(inactiveMembers.toArray(new Node[inactiveMembers.size()]));
    }

    /**
     * Initiate the bootstrapping of the weaver ring.
     */
    public void initiateBootstrap(Node[] joiningMembers) {
        if (!isInactiveLeader()) {
            throw new IllegalStateException(
                                            "This node must be inactive and the leader to initiate rebalancing");
        }
        if (activeMembers.size() != 0) {
            throw new IllegalStateException(
                                            "There must be no active members in the partition");
        }
        if (joiningMembers == null) {
            throw new IllegalArgumentException(
                                               "joining members must not be null");
        }
        for (Node node : joiningMembers) {
            if (!inactiveMembers.contains(node)) {
                throw new IllegalArgumentException(
                                                   "Joining members must be inactive");
            }
        }
        fsm.bootstrapSystem(joiningMembers);
    }

    /**
     * Initiate the rebalancing of the weaver ring using the set of inactive
     * members
     */
    public void initiateRebalance() {
        initiateRebalance(inactiveMembers.toArray(new Node[inactiveMembers.size()]));
    }

    /**
     * Initiate the rebalancing of the weaver ring.
     */
    public void initiateRebalance(Node[] joiningMembers) {
        if (!isActiveLeader()) {
            throw new IllegalStateException(
                                            "This node must be active and the leader to initiate rebalancing");
        }
        if (joiningMembers == null) {
            throw new IllegalArgumentException(
                                               "joining members must not be null");
        }
        for (Node node : joiningMembers) {
            if (!inactiveMembers.contains(node)) {
                throw new IllegalArgumentException(
                                                   "Joining members must be inactive");
            }
        }
        fsm.rebalance(joiningMembers);
    }

    /**
     * Open the new channel if this node is a primary or mirror of the new
     * channel.
     * 
     * @param channel
     *            - the id of the channel to open
     */
    public void open(UUID channel) {
        if (!active) {
            return;
        }
        Node[] pair = weaver.getReplicationPair(channel);
        if (self.equals(pair[0])) {
            // This node is the primary
            if (activeMembers.contains(pair[1])) {
                weaver.openPrimary(channel, pair[1]);
            } else {
                weaver.openPrimary(channel, null);
                switchboard.ringCast(new Message(self,
                                                 ChannelMessage.MIRROR_OPENED,
                                                 channel));
            }
            switchboard.ringCast(new Message(self,
                                             ChannelMessage.PRIMARY_OPENED,
                                             channel));
        } else if (self.equals(pair[1])) {
            // This node is the secondary.
            if (activeMembers.contains(pair[0])) {
                // Primary is active
                weaver.openMirror(channel);
            } else {
                weaver.openPrimary(channel, null);
                switchboard.ringCast(new Message(self,
                                                 ChannelMessage.PRIMARY_OPENED,
                                                 channel));
            }
            switchboard.ringCast(new Message(self,
                                             ChannelMessage.MIRROR_OPENED,
                                             channel));
        }
    }

    @Override
    public void stabilized() {
        filterSystemMembership();
        fsm.stabilize();
    }

    @Override
    public String toString() {
        return String.format("Coordinator for spindle [%s]", self.processId);
    }

    /**
     * indicates which end initiates a connection
     * 
     * @param target
     *            - the other end of the intended connection
     * @return - true if this end initiates the connection, false otherwise
     */
    private boolean thisEndInitiatesConnectionsTo(Node target) {
        return self.compareTo(target) < 0;
    }

    protected void beginRebalance(Node[] joiningMembers) {
        setJoiningMembers(joiningMembers);
        switchboard.ringCast(new Message(self,
                                         ReplicatorMessage.BEGIN_REBALANCE),
                             nextMembership);
    }

    protected void bootstrap(Node[] bootsrappingMembers) {
        active = true;
        setJoiningMembers(bootsrappingMembers);
        for (Node node : bootsrappingMembers) {
            activeMembers.add(node);
            inactiveMembers.remove(node);
        }
        weaver.bootstrap(bootsrappingMembers);
    }

    /**
     * Calculate the next ring based on the current member set and the new
     * member set
     */
    protected void calculateNextRing() {
        ConsistentHashFunction<Node> newRing = new ConsistentHashFunction<Node>();
        for (Node node : activeMembers) {
            newRing.add(node, node.capacity);
        }
        for (Node node : joiningMembers) {
            newRing.add(node, node.capacity);
        }
        weaver.setNextRing(newRing);
    }

    protected void cleanUp() {
        if (rendezvous != null) {
            rendezvous.cancel();
            rendezvous = null;
        }
        weaver.setNextRing(null);
        tally.set(0);
        joiningMembers = new Node[0];
        nextMembership.clear();
    }

    protected void commitFailover() {
        switchboard.ringCast(new Message(self, FailoverMessage.FAILOVER));
    }

    /**
     * Commit the calculated next ring as the current weaver ring
     */
    protected void commitNextRing() {
        weaver.commitRing();
    }

    /**
     * Commit the takeover of the new primaries and secondaries.
     */
    protected void commitTakeover() {
        switchboard.ringCast(new Message(self,
                                         RebalanceMessage.REBALANCE_COMPLETE));
    }

    protected void coordinateBootstrap() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating bootstrap on %s", self));
        }
        tally.set(nextMembership.size());
        switchboard.ringCast(new Message(self,
                                         BootstrapMessage.BOOTSTRAP_SPINDLES,
                                         (Serializable) joiningMembers));
    }

    protected void coordinateFailover() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating weaver failover on %s", self));
        }
        switchboard.ringCast(new Message(self, FailoverMessage.PREPARE),
                             activeMembers);
    }

    /**
     * The receiver is the controller for the group. Coordinate the rebalancing
     * of the system by including the new members.
     */
    protected void coordinateRebalance() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating rebalancing on %s", self));
        }
        tally.set(nextMembership.size());
        switchboard.ringCast(new Message(
                                         self,
                                         RebalanceMessage.PREPARE_FOR_REBALANCE,
                                         (Serializable) joiningMembers));
    }

    /**
     * The receiver is the controller for the group. Coordinate the
     * establishment of the replicators on the members and new members of the
     * group
     */
    protected void coordinateReplicators() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating establishment of the replicators on %s",
                                   self));
        }
        tally.set(nextMembership.size());
        Message message = new Message(self,
                                      ReplicatorMessage.ESTABLISH_REPLICATORS);
        switchboard.ringCast(message, nextMembership);
    }

    /**
     * Coordinate the takeover of the completion of the rebalancing
     */
    protected void coordinateTakeover() {
        switchboard.ringCast(new Message(self, RebalanceMessage.TAKEOVER));
    }

    /**
     * A fatal state exception occurred, so destabilize the partition
     */
    protected void destabilizePartition() {
        switchboard.destabilize();
    }

    /**
     * Open the replicators to the the new members.
     */
    protected void establishReplicators() {
        rendezvous = null;

        Runnable rendezvousAction = new Runnable() {
            @Override
            public void run() {
                rendezvous = null;
                if (log.isLoggable(Level.FINE)) {
                    log.fine(String.format("Replicators established on %s",
                                           self));
                }
                if (isActiveLeader()) {
                    tally.decrementAndGet();
                } else {
                    switchboard.ringCast(new Message(
                                                     self,
                                                     ReplicatorMessage.REPLICATORS_ESTABLISHED),
                                         nextMembership);
                }
                fsm.replicatorsEstablished();
            }
        };

        // Only make the outbound connections this node initiates, and ignore the loopback connection
        ArrayList<Node> contacts = new ArrayList<Node>();
        for (Node node : joiningMembers) {
            if (!node.equals(self) && thisEndInitiatesConnectionsTo(node)) {
                contacts.add(node);
            }
        }

        if (contacts.isEmpty()) {
            rendezvousAction.run();
        }

        final ArrayList<Replicator> replicators = new ArrayList<Replicator>();
        Runnable cancellationAction = new Runnable() {
            @Override
            public void run() {
                rendezvous = null;
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicator establishment cancelled on %s",
                                           self));
                }
                for (Replicator replicator : replicators) {
                    if (replicator.getState() != ReplicatorFSM.Established) {
                        replicator.close();
                    }
                }
                fsm.replicatorsEstablishmentCancelled();
            }
        };
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Establishment of replicators initiated on %s",
                                    self));
        }
        rendezvous = new Rendezvous(contacts.size(), rendezvousAction,
                                    cancellationAction);
        for (Node member : contacts) {
            replicators.add(weaver.openReplicator(member,
                                                  yellowPages.get(member),
                                                  rendezvous));
        }
        rendezvous.scheduleCancellation(DEFAULT_TIMEOUT, TIMEOUT_UNIT, timer);
        weaver.connectReplicators(replicators, yellowPages);
    }

    /**
     * The weaver membership has partitioned. Failover any channels the dead
     * members were serving as primary that the receiver's node is providing
     * mirrors for.
     */
    protected void failover() {
        Collection<Node> deadMembers = switchboard.getDeadMembers();
        for (Node node : deadMembers) {
            yellowPages.remove(node);
        }
        weaver.failover(deadMembers);
        fsm.failedOver();
    }

    /**
     * Filter the system membership to reflect any dead members that did not
     * survive to this partition incarnation
     */
    protected void filterSystemMembership() {
        Collection<Node> deadMembers = switchboard.getDeadMembers();
        activeMembers.removeAll(deadMembers);
        inactiveMembers.removeAll(deadMembers);
        nextMembership.clear();
        nextMembership.addAll(activeMembers);
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Filtered membership on %s, active = %s, inactive = %s",
                                    self, activeMembers, inactiveMembers));
        }
    }

    protected SortedSet<Node> getNextMembership() {
        return nextMembership;
    }

    protected boolean hasActiveMembers() {
        return !activeMembers.isEmpty();
    }

    protected boolean isActive() {
        return active;
    }

    /**
     * Answer true if the receiver is active and the leader of the active group
     * 
     * @return
     */
    protected boolean isActiveLeader() {
        if (active) {
            return activeMembers.isEmpty() ? true
                                          : activeMembers.last().equals(self);
        }
        return false;
    }

    /**
     * Answer true if the receiver is not active and the leader of the inactive
     * group
     * 
     * @return
     */
    protected boolean isInactiveLeader() {
        if (!active) {
            return inactiveMembers.isEmpty() ? true
                                            : inactiveMembers.last().equals(self);
        }
        return false;
    }

    protected void printState() {
        System.out.println(String.format("Coordinator [%s], active=%s, active=%s, inactive=%s, dead=%s",
                                         self.processId, active, activeMembers,
                                         inactiveMembers,
                                         switchboard.getDeadMembers()));
    }

    /**
     * Rebalance the weaver process group.
     */
    protected void rebalance() {
        rebalance(weaver.remap(), switchboard.getDeadMembers());
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
    protected void rebalance(Map<UUID, Node[][]> remapped,
                             Collection<Node> deadMembers) {
        final Map<Node, Xerox> xeroxes = new HashMap<Node, Xerox>();

        Runnable rendezvousAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weavers rebalanced on %s", self));
                }
                for (Xerox xerox : xeroxes.values()) {
                    xerox.close();
                }
                fsm.rebalanced();
                switchboard.ringCast(new Message(
                                                 self,
                                                 RebalancedMessage.MEMBER_REBALANCED),
                                     nextMembership);

            }
        };

        Runnable cancellationAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weaver rebalancing cancelled on %s",
                                           self));
                }
                for (Xerox xerox : xeroxes.values()) {
                    xerox.close();
                }
                fsm.rebalanceCancelled();
            }
        };

        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Establishment of replicators initiated on %s",
                                   self));
        }
        for (Entry<UUID, Node[][]> entry : remapped.entrySet()) {
            weaver.rebalance(xeroxes, entry.getKey(), entry.getValue()[0][0],
                             entry.getValue()[0][1], entry.getValue()[1][0],
                             entry.getValue()[1][1], deadMembers);
        }

        if (xeroxes.isEmpty()) {
            rendezvousAction.run();
        }

        rendezvous = new Rendezvous(xeroxes.size(), rendezvousAction,
                                    cancellationAction);
        weaver.connectXeroxes(xeroxes, yellowPages, rendezvous);
    }

    /**
     * Calculate the rebalancing of the system using the supplied list of
     * joining weaver processes.
     * 
     * @param joiningMembers
     *            - the list of weavers that are joining the process group
     */
    protected void rebalance(Node[] joiningMembers) {
        setJoiningMembers(joiningMembers);
        calculateNextRing();
        fsm.rebalance();
    }

    /**
     * The weaver cluster has been rebalanced. Switch over to the new membership
     * and commit the takeover
     */
    protected void rebalanced() {
        activeMembers.clear();
        activeMembers.addAll(nextMembership);
        inactiveMembers.removeAll(nextMembership);
        joiningMembers = new Node[0];
        commitNextRing();
        active = true;
        fsm.commitTakeover();
    }

    /**
     * Note that the replicators have been established on the weaver process
     * group member
     * 
     * @param member
     */
    protected void replicatorsEstablished(Node member) {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Replicators reported established on %s (coordinator %s)",
                                   member, self));
        }
        tally.decrementAndGet();
        fsm.replicatorsEstablished();
    }

    /**
     * @return true if the tally is equal to the required size
     */
    protected boolean tallyComplete() {
        return tally.get() == 0;
    }

    /**
     * Test access
     * 
     * @return the set of active members
     */
    SortedSet<Node> getActiveMembers() {
        return activeMembers;
    }

    /**
     * Test access
     * 
     * @return the FSM for the receiver
     */
    CoordinatorContext getFsm() {
        return fsm;
    }

    /**
     * Test access
     * 
     * @return the set of inactive members
     */
    SortedSet<Node> getInactiveMembers() {
        return inactiveMembers;
    }

    /**
     * @return the current rendenzvous of the coordinator
     */
    Rendezvous getRendezvous() {
        return rendezvous;
    }

    /**
     * Test access to set the active state of the receiver
     * 
     * @param active
     */
    void setActive(boolean active) {
        this.active = active;
    }

    /**
     * Set the joining members of the receiver
     * 
     * @param joiningMembers
     */
    void setJoiningMembers(Node[] joiningMembers) {
        assert joiningMembers != null : "joining members must not be null";
        this.joiningMembers = joiningMembers;
        for (Node node : joiningMembers) {
            nextMembership.add(node);
        }
    }

    /**
     * Set the consistent hash function for the next weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    void setNextRing(ConsistentHashFunction<Node> ring) {
        weaver.setNextRing(ring);
    }

    protected void rebalancePrepared() {
        rendezvous = null;
        switchboard.ringCast(new Message(self,
                                         RebalanceMessage.INITIATE_REBALANCE));
    }
}
