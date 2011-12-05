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
import java.nio.channels.SocketChannel;
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

import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
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
import com.salesforce.ouroboros.spindle.transfer.Sink;
import com.salesforce.ouroboros.spindle.transfer.Xerox;
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
    private class SinkFactory implements CommunicationsHandlerFactory {
        @Override
        public Sink createCommunicationsHandler(SocketChannel channel) {
            return new Sink(weaver);
        }
    }

    private final static Logger                 log             = Logger.getLogger(Coordinator.class.getCanonicalName());
    private static final String                 WEAVER_XEROX    = "Weaver Xerox";
    static final int                            DEFAULT_TIMEOUT = 1;
    static final TimeUnit                       TIMEOUT_UNIT    = TimeUnit.MINUTES;

    private boolean                             active          = false;
    private final SortedSet<Node>               activeMembers   = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               allMembers      = new ConcurrentSkipListSet<Node>();
    private final CoordinatorContext            fsm             = new CoordinatorContext(
                                                                                         this);
    private final Node                          id;
    private final SortedSet<Node>               inactiveMembers = new ConcurrentSkipListSet<Node>();
    private Node[]                              joiningMembers  = new Node[0];
    private ConsistentHashFunction<Node>        nextRing;
    private Rendezvous                          rendezvous;
    private final Switchboard                   switchboard;
    private final AtomicInteger                 tally           = new AtomicInteger();
    private int                                 targetTally;
    private final ScheduledExecutorService      timer;
    private final Weaver                        weaver;
    private final ServerSocketChannelHandler    xeroxHandler;
    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(ScheduledExecutorService timer, Switchboard switchboard,
                       Weaver weaver, CoordinatorConfiguration configuration)
                                                                             throws IOException {
        this.timer = timer;
        this.switchboard = switchboard;
        this.weaver = weaver;
        switchboard.setMember(this);
        id = weaver.getId();
        yellowPages.put(id, weaver.getContactInformation());
        xeroxHandler = new ServerSocketChannelHandler(
                                                      WEAVER_XEROX,
                                                      configuration.getXeroxSocketOptions(),
                                                      configuration.getXeroxAddress(),
                                                      configuration.getXeroxes(),
                                                      new SinkFactory());
    }

    @Override
    public void advertise() {
        ContactInformation info = yellowPages.get(id);
        switchboard.ringCast(new Message(
                                         weaver.getId(),
                                         DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER,
                                         info, active));
    }

    /**
     * Close the channel if the node is a primary or mirror of the existing
     * channel.
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel, Node requester) {
        Node[] pair = weaver.getReplicationPair(channel);
        if (activeMembers.contains(pair[0])) {
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
        weaver.close(channel);
        switchboard.send(new Message(id, ChannelMessage.CLOSED, channel),
                         requester);
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
                bootstrap((Node[]) arguments[0]);
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type, arguments));
                }
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
                open((UUID) arguments[0], sender);
                break;
            }
            case OPEN_MIRROR: {
                @SuppressWarnings("unchecked")
                Association<Node, UUID> ass = (Association<Node, UUID>) arguments[0];
                openMirror(ass.value, ass.key);
                break;
            }
            case CLOSE: {
                close((UUID) arguments[0], sender);
                break;
            }
            case CLOSE_MIRROR: {
                @SuppressWarnings("unchecked")
                Association<Node, UUID> ass = (Association<Node, UUID>) arguments[0];
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
    public void dispatch(DiscoveryMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                allMembers.add(sender);
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
                if (isLeader()) {
                    tally.incrementAndGet();
                    fsm.memberRebalanced();
                } else {
                    switchboard.forwardToNextInRing(new Message(sender, type,
                                                                arguments),
                                                    allMembers);
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
            case READY_REPLICATORS:
                readyReplicators();
                if (isLeader()) {
                    fsm.replicatorsReady();
                }
                break;
            case REPLICATORS_ESTABLISHED:
                if (isLeader()) {
                    replicatorsEstablished(sender);
                } else {
                    switchboard.forwardToNextInRing(new Message(sender, type,
                                                                arguments),
                                                    allMembers);
                }
                break;
            case CONNECT_REPLICATORS:
                weaver.connectReplicators(yellowPages);
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid replicator message: %s",
                                                              type));
        }
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
        if (!isLeader() || active) {
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
        if (!isLeader() || !active) {
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
    public void open(UUID channel, Node requester) {
        Node[] pair = weaver.getReplicationPair(channel);
        if (!weaver.openPrimary(channel, pair[1])) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Channel is already opened %s", channel));
            }
            return;
        } else if (activeMembers.contains(pair[1])) {
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
    }

    /**
     * Open the new channel if this node is a primary or mirror of the new
     * channel.
     * 
     * @param channel
     *            - the id of the channel to open
     */
    public void openMirror(UUID channel, Node requester) {
        Node[] pair = weaver.getReplicationPair(channel);
        if (!weaver.openMirror(channel, pair[0])) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Channel is already opened on mirror %s",
                                        channel));
            }
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Opening mirror for channel %s on %s",
                                       channel, id));
            }
        }
        switchboard.send(new Message(id, ChannelMessage.OPENED, channel),
                         requester);
    }

    @Override
    public void stabilized() {
        fsm.stabilize();
    }

    public void start() {
        xeroxHandler.start();
    }

    public void terminate() {
        xeroxHandler.terminate();
    }

    protected void bootstrap(Node[] bootsrappingMembers) {
        active = true;
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
        nextRing = newRing;
    }

    protected void cleanUp() {
        if (rendezvous != null) {
            rendezvous.cancel();
            rendezvous = null;
        }
        nextRing = null;
        tally.set(0);
        joiningMembers = new Node[0];
    }

    protected void commitFailover() {
        switchboard.ringCast(new Message(id, FailoverMessage.FAILOVER));
    }

    /**
     * Commit the calculated next ring as the current weaver ring
     */
    protected void commitNextRing() {
        weaver.setRing(nextRing);
        nextRing = null;
    }

    /**
     * Commit the takeover of the new primaries and secondaries.
     */
    protected void commitTakeover() {
        // TODO
    }

    protected void connectReplicators() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating replicators connect on %s", id));
        }
        switchboard.ringCast(new Message(id,
                                         ReplicatorMessage.CONNECT_REPLICATORS),
                             allMembers);
    }

    protected void coordinateBootstrap() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating bootstrap on %s", id));
        }
        switchboard.ringCast(new Message(id,
                                         BootstrapMessage.BOOTSTRAP_SPINDLES,
                                         (Serializable) joiningMembers));
    }

    protected void coordinateFailover() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating weaver failover on %s", id));
        }
        switchboard.ringCast(new Message(id, FailoverMessage.PREPARE),
                             activeMembers);
    }

    /**
     * The receiver is the controller for the group. Coordinate the rebalancing
     * of the system by including the new members.
     */
    protected void coordinateRebalance() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating rebalancing on %s", id));
        }
        switchboard.ringCast(new Message(
                                         id,
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
                                   id));
        }
        tally.set(0);
        targetTally = allMembers.size();
        Message message = new Message(id, ReplicatorMessage.READY_REPLICATORS);
        switchboard.ringCast(message, allMembers);
    }

    /**
     * Coordinate the takeover of the completion of the rebalancing
     */
    protected void coordinateTakeover() {
        rendezvous = null;
        switchboard.ringCast(new Message(id,
                                         RebalanceMessage.INITIATE_REBALANCE),
                             allMembers);
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
            yellowPages.remove(node);
        }
        weaver.failover(deadMembers);
        filterSystemMembership();
        fsm.failedOver();
    }

    /**
     * Filter the system membership to reflect any dead members that did not
     * survive to this partition incarnation
     */
    protected void filterSystemMembership() {
        Collection<Node> deadMembers = switchboard.getDeadMembers();
        allMembers.removeAll(deadMembers);
        activeMembers.removeAll(deadMembers);
        inactiveMembers.removeAll(deadMembers);
    }

    protected boolean isActive() {
        return active;
    }

    /**
     * Answer true if the receiver is the leader of the group
     * 
     * @return
     */
    protected boolean isLeader() {
        if (active) {
            return activeMembers.size() == 0 ? true
                                            : activeMembers.last().equals(id);
        }
        return inactiveMembers.size() == 0 ? true
                                          : inactiveMembers.last().equals(id);
    }

    /**
     * Open the replicators to the the new members
     */
    protected void readyReplicators() {
        rendezvous = null;

        Runnable rendezvousAction = new Runnable() {
            @Override
            public void run() {
                rendezvous = null;
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicators established on %s", id));
                }
                switchboard.ringCast(new Message(
                                                 id,
                                                 ReplicatorMessage.REPLICATORS_ESTABLISHED),
                                     allMembers);
                fsm.replicatorsEstablished();
            }
        };

        // Can't replicate back to self
        ArrayList<Node> contacts = new ArrayList<Node>(inactiveMembers);
        contacts.remove(id);

        if (contacts.isEmpty()) {
            rendezvousAction.run();
            return;
        }

        final ArrayList<Replicator> replicators = new ArrayList<Replicator>();
        Runnable cancellationAction = new Runnable() {
            @Override
            public void run() {
                rendezvous = null;
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicator establishment cancelled on %s",
                                           id));
                }
                for (Replicator replicator : replicators) {
                    if (replicator.getState() != ReplicatorFSM.Established) {
                        replicator.close();
                    }
                }
                fsm.replicatorsEstablishmentCancelled();
            }
        };
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Establishment of replicators initiated on %s",
                                   id));
        }
        rendezvous = new Rendezvous(contacts.size(), rendezvousAction,
                                    cancellationAction);
        for (Node member : contacts) {
            replicators.add(weaver.openReplicator(member,
                                                  yellowPages.get(member),
                                                  rendezvous));
        }
        rendezvous.scheduleCancellation(DEFAULT_TIMEOUT, TIMEOUT_UNIT, timer);

        fsm.replicatorsReady();
    }

    /**
     * Rebalance the weaver process group.
     */
    protected void rebalance() {
        rebalance(weaver.remap(nextRing), switchboard.getDeadMembers());
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
                    log.info(String.format("Weavers rebalanced on %s", id));
                }
                for (Xerox xerox : xeroxes.values()) {
                    xerox.close();
                }
                fsm.rebalanced();
            }
        };

        Runnable cancellationAction = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Weaver rebalancing cancelled on %s",
                                           id));
                }
                for (Xerox xerox : xeroxes.values()) {
                    xerox.close();
                }
                fsm.rebalanceCancelled();
            }
        };

        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Establishment of replicators initiated on %s",
                                   id));
        }

        rendezvous = new Rendezvous(xeroxes.size(), rendezvousAction,
                                    cancellationAction);

        for (Entry<UUID, Node[][]> entry : remapped.entrySet()) {
            weaver.rebalance(xeroxes, rendezvous, entry.getKey(),
                             entry.getValue()[0], entry.getValue()[1],
                             deadMembers);
        }

        for (Map.Entry<Node, Xerox> entry : xeroxes.entrySet()) {
            try {
                xeroxHandler.connectTo(yellowPages.get(entry.getKey()).xerox,
                                       entry.getValue());
            } catch (IOException e) {
                cancellationAction.run();
                return;
            }
        }
    }

    /**
     * Calculate the rebalancing of the system using the supplied list of
     * joining weaver processes.
     * 
     * @param joiningMembers
     *            - the list of weavers that are joining the process group
     */
    protected void rebalance(Node[] joiningMembers) {
        this.joiningMembers = joiningMembers;
        calculateNextRing();
        fsm.rebalance();
    }

    /**
     * The weaver cluster has been rebalanced. Switch over to the new membership
     * and commit the takeover
     */
    protected void rebalanced() {
        for (Node node : joiningMembers) {
            activeMembers.add(node);
            inactiveMembers.remove(node);
        }
        commitNextRing();
        active = true;
        switchboard.ringCast(new Message(id,
                                         RebalancedMessage.MEMBER_REBALANCED),
                             allMembers);
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
                                   member, id));
        }
        tally.incrementAndGet();
        fsm.replicatorsEstablished();
    }

    /**
     * @return true if the tally is equal to the required size
     */
    protected boolean tallyComplete() {
        return tally.intValue() == targetTally;
    }

    /**
     * Test access
     * 
     * @return the set of active members
     */
    SortedSet<Node> getActiveMembers() {
        return activeMembers;
    }

    SortedSet<Node> getAllMembers() {
        return allMembers;
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
     * Set the joining members of the receiver
     * 
     * @param joiningMembers
     */
    void setJoiningMembers(Node[] joiningMembers) {
        assert joiningMembers != null : "joining members must not be null";
        this.joiningMembers = joiningMembers;
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
