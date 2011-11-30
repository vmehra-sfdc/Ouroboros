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

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SocketChannel;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import statemap.StateUndefinedException;

import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.salesforce.ouroboros.ChannelMessage;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.RebalanceMessage;
import com.salesforce.ouroboros.partition.FailoverMessage;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
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
    private final Set<UUID>                     channels        = new HashSet<UUID>();
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
    private ConsistentHashFunction<Node>        weaverRing      = new ConsistentHashFunction<Node>();
    private final ServerSocketChannelHandler    xeroxHandler;
    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(ScheduledExecutorService timer, Switchboard switchboard,
                       Weaver weaver, CoordinatorConfiguration configuration)
                                                                             throws IOException {
        this.timer = timer;
        this.switchboard = switchboard;
        this.weaver = weaver;
        switchboard.setMember(this);
        weaver.setCoordinator(this);
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
                                         GlobalMessageType.ADVERTISE_CHANNEL_BUFFER,
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
        channels.remove(channel);
        Node[] pair = getReplicationPair(channel);
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
    public void dispatch(FailoverMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PREPARE:
                failover();
                if (isLeader()) {
                    switchboard.ringCast(new Message(sender,
                                                     FailoverMessage.FAILOVER));
                } else {
                    switchboard.ringCast(new Message(sender,
                                                     FailoverMessage.PREPARE),
                                         activeMembers);
                }
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

    @Override
    public void dispatch(GlobalMessageType type, Node sender,
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

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.MemberDispatch, com.salesforce.ouroboros.Node, java.io.Serializable, long)
     */
    @Override
    public void dispatch(MemberDispatch type, Node sender,
                         Serializable[] arguments, long time) {
    }

    @Override
    public void dispatch(RebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case BOOTSTRAP:
                bootstrap((Node[]) arguments[0]);
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type, arguments));
                }
                fsm.bootstrapped();
                break;
            case PREPARE_FOR_REBALANCE:
                rebalance((Node[]) arguments[0]);
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type, arguments));
                }
                fsm.rebalance();
                break;
            case INITIATE_REBALANCE:
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type));
                }
                break;
            case REBALANCE_COMPLETE:
                rebalanced();
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type));
                }
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
                if (isLeader()) {
                    replicatorsReady();
                } else {
                    readyReplicators();
                    switchboard.ringCast(new Message(sender, type, arguments),
                                         allMembers);
                }
                break;
            case REPLICATORS_ESTABLISHED:
                if (isLeader()) {
                    replicatorsEstablished(sender);
                } else {
                    switchboard.ringCast(new Message(sender, type, arguments),
                                         allMembers);
                }
                break;
            case CONNECT_REPLICATORS:
                if (!isLeader()) {
                    switchboard.ringCast(new Message(sender, type, arguments),
                                         allMembers);
                }
                weaver.connectReplicators(yellowPages);
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid replicator message: %s",
                                                              type));
        }
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
        Node[] pair = getReplicationPair(channel);
        if (!channels.add(channel)) {
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
            weaverRing.add(node, node.capacity);
        }
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

    /**
     * Commit the calculated next ring as the current weaver ring
     */
    protected void commitNextRing() {
        assert nextRing != null : "Next ring has not been calculated";
        weaverRing = nextRing;
        nextRing = null;
    }

    /**
     * Commit the takeover of the new primaries and secondaries.
     */
    protected void commitTakeover() {
        assert isLeader() : "Must be leader to commit takeover";
        // TODO Auto-generated method stub

    }

    protected void connectReplicators() {
        assert isLeader() : "Must be leader to coordinate replicator connect";
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating replicators connect on %s", id));
        }
        tally.set(0);
        targetTally = allMembers.size();
        switchboard.ringCast(new Message(id,
                                         ReplicatorMessage.CONNECT_REPLICATORS),
                             allMembers);
    }

    protected void coordinateBootstrap() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating bootstrap on %s", id));
        }
        switchboard.ringCast(new Message(id, RebalanceMessage.BOOTSTRAP,
                                         (Serializable) joiningMembers));
    }

    protected void coordinateFailover() {
        assert isLeader() : "Must be leader to coordinate the rebalance";
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
        assert isLeader() : "Must be leader to coordinate the rebalance";
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
        assert isLeader() : "Must be leader to coordinate the establishment of replicators";
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating establishment of the replicators on %s",
                                   id));
        }
        Message message = new Message(id, ReplicatorMessage.READY_REPLICATORS);
        switchboard.ringCast(message, allMembers);
    }

    protected void coordinateTakeover() {
        assert isLeader() : "Must be leader to coordinate the takeover";
        rendezvous = null;
        switchboard.ringCast(new Message(id,
                                         RebalanceMessage.REBALANCE_COMPLETE),
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
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Removing weaver[%s] from the partition",
                                       node));
            }
            yellowPages.remove(node);
            weaver.closeReplicator(node);
        }
        weaver.failover(deadMembers);
        filterSystemMembership();
        fsm.failedOver();
    }

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
     * 
     * @param controller
     *            - the controller for the group
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
                if (isLeader()) {
                    tally.incrementAndGet();
                } else {
                    switchboard.ringCast(new Message(
                                                     id,
                                                     ReplicatorMessage.REPLICATORS_ESTABLISHED),
                                         allMembers);
                }
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

    protected void rebalance() {
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
        weaverRing = nextRing;
        active = true;
        fsm.commitTakeover();
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

    protected void replicatorsEstablished(Node member) {
        tally.incrementAndGet();
        fsm.replicatorsEstablished();
    }

    protected void replicatorsReady() {
        fsm.replicatorsReady();
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
     * @return the list of active members
     */
    Collection<Node> getActiveMembers() {
        return activeMembers;
    }

    CoordinatorContext getFsm() {
        return fsm;
    }

    SortedSet<Node> getInactiveMembers() {
        return inactiveMembers;
    }

    /**
     * @return the current rendenzvous of the coordinator
     */
    Rendezvous getRendezvous() {
        return rendezvous;
    }

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
