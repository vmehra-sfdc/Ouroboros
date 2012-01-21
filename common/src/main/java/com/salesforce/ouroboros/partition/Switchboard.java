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
package com.salesforce.ouroboros.partition;

import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.comms.MessageConnection;
import org.smartfrog.services.anubis.partition.util.NodeIdSet;
import org.smartfrog.services.anubis.partition.views.View;

import statemap.StateUndefinedException;
import statemap.TransitionUndefinedException;

import com.fasterxml.uuid.NoArgGenerator;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardState;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.ViewElectionMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;
import com.salesforce.ouroboros.util.Gate;

/**
 * The common high level distribute coordination logic for Ouroboros group
 * members. Provides the basic handling for handling partitions, maintaining
 * group membership, accounting for dead members after a partition change, and
 * the message passing logic.
 * 
 * @author hhildebrand
 * 
 */
public class Switchboard {
    public interface Member {
        /**
         * Advertise the receiver service, by ring casting the service across
         * the membership ring
         */
        void advertise();

        /**
         * The member is part of a new view and must start in the inactive
         * state.
         */
        void becomeInactive();

        /**
         * The partition has been destabilized
         */
        void destabilize();

        void dispatch(BootstrapMessage type, Node sender,
                      Serializable[] arguments, long time);

        void dispatch(ChannelMessage type, Node sender,
                      Serializable[] arguments, long time);

        void dispatch(DiscoveryMessage type, Node sender,
                      Serializable[] arguments, long time);

        void dispatch(FailoverMessage type, Node sender,
                      Serializable[] arguments, long time);

        void dispatch(WeaverRebalanceMessage type, Node sender,
                      Serializable[] arguments, long time);

        /**
         * The partition has stabilized.
         */
        void stabilized();

    }

    public static class Result implements Comparable<Result> {
        public final int  tally;
        public final UUID vote;

        public Result(int tally, UUID vote) {
            super();
            this.tally = tally;
            this.vote = vote;
        }

        @Override
        public int compareTo(Result other) {
            int tallyCompare = tally - other.tally;
            if (tallyCompare == 0) {
                return vote.compareTo(other.vote);
            }
            return tallyCompare;
        }
    }

    class Notification implements PartitionNotification {

        @Override
        public void objectNotification(final Object obj, final int sender,
                                       final long time) {
            if (obj instanceof Message) {
                try {
                    messageProcessor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processMessage((Message) obj, sender, time);
                            } catch (TransitionUndefinedException e) {
                                if (log.isLoggable(Level.WARNING)) {
                                    log.log(Level.WARNING,
                                            String.format("Transition error processing message: %s from: %s on %s",
                                                          obj, sender,
                                                          self.processId), e);
                                }
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest(String.format("rejecting message %s due to shutdown on %s",
                                                 obj, self));
                    }
                }
            }
        }

        @Override
        public void partitionNotification(final View view, final int leader) {
            partitionEvent(view, leader);
        }
    }

    private static final Logger         log             = Logger.getLogger(Switchboard.class.getCanonicalName());

    private final ArrayList<Node>       deadMembers     = new ArrayList<Node>();
    private final SwitchboardContext    fsm             = new SwitchboardContext(
                                                                                 this);
    private final Gate                  inboundGate     = new Gate();
    private final NoArgGenerator        viewIdGenerator;
    private volatile boolean            leader          = false;
    private Member                      member;
    private SortedSet<Node>             members         = new ConcurrentSkipListSet<Node>();
    private final Executor              messageProcessor;
    private final PartitionNotification notification    = new Notification();
    private final Partition             partition;
    private ArrayList<Node>             previousMembers = new ArrayList<Node>();
    private NodeIdSet                   previousView;
    private final Node                  self;
    private final AtomicBoolean         stable          = new AtomicBoolean(
                                                                            false);
    private NodeIdSet                   view;
    private UUID                        viewId;

    public Switchboard(Node node, Partition p, NoArgGenerator viewIdGenerator) {
        inboundGate.close();
        self = node;
        fsm.setName(Integer.toString(self.processId));
        partition = p;
        messageProcessor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread daemon = new Thread(
                                           r,
                                           String.format("Message processor for %s",
                                                         self));
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.log(Level.SEVERE,
                                String.format("Uncaught exception on message processing thread on %s",
                                              self), e);
                    }
                });
                return daemon;
            }
        });
        this.viewIdGenerator = viewIdGenerator;
        viewId = this.viewIdGenerator.generate();
    }

    /**
     * Broadcast the message to all the members in our partition
     * 
     * @param msg
     *            - the message to broadcast
     */
    public void broadcast(Message msg) {
        broadcast(msg, members);
    }

    /**
     * Broadcast the message to all the members in the target group
     * 
     * @param msg
     *            - the message to broadcast
     * @param target
     *            - the target group of nodes to receive the message
     */
    public void broadcast(Message msg, Collection<Node> target) {
        for (Node node : target) {
            send(msg, node);
        }
    }

    /**
     * Force the partition to destabilize
     */
    public void destabilize() {
        partition.destabilize();
    }

    /**
     * The double dispatch of the GlobalMessage
     * 
     * @param type
     * @param sender
     * @param arguments
     * @param time
     */
    public void dispatch(DiscoveryMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case DISCOVERY_COMPLETE:
                if (log.isLoggable(Level.FINE)) {
                    log.fine(String.format("Discovery complete on %s", self));
                }
                fsm.discoveryComplete();
                break;
            case ADVERTISE_NOOP:
                if (log.isLoggable(Level.FINER)) {
                    log.finer(String.format("Discovery of noop node %s = %s",
                                            self, type));
                }
                discover(sender);
                break;
            default:
                if (log.isLoggable(Level.FINER)) {
                    log.finer(String.format("Discovery of node %s = %s", self,
                                            type));
                }
                discover(sender);
                member.dispatch(type, sender, arguments, time);
                break;
        }
    }

    public void dispatch(ViewElectionMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case VOTE:
                assert arguments.length > 0 : "No view ids in vote";
                if (isLeader()) {
                    fsm.votingComplete((UUID[]) arguments);
                } else {
                    Serializable[] votes = Arrays.copyOf(arguments,
                                                         arguments.length + 1);
                    votes[arguments.length] = viewId;
                    forwardToNextInRing(new Message(sender,
                                                    ViewElectionMessage.VOTE,
                                                    votes));
                }
                break;
            case NEW_VIEW_ID:
                assert arguments.length == 2 : "Incorrect number of ids in new view message";
                if (!((UUID) arguments[0]).equals(viewId)) {
                    member.becomeInactive();
                }
                viewId = (UUID) arguments[1];
                fsm.viewEstablished();
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid view election message type: %s",
                                                              type));
        }
    }

    public void dispatchToMember(BootstrapMessage type, Node sender,
                                 Serializable[] arguments, long time) {
        member.dispatch(type, sender, arguments, time);
    }

    public void dispatchToMember(ChannelMessage type, Node sender,
                                 Serializable[] arguments, long time) {
        member.dispatch(type, sender, arguments, time);
    }

    public void dispatchToMember(FailoverMessage type, Node sender,
                                 Serializable[] arguments, long time) {
        member.dispatch(type, sender, arguments, time);
    }

    public void dispatchToMember(WeaverRebalanceMessage type, Node sender,
                                 Serializable[] arguments, long time) {
        member.dispatch(type, sender, arguments, time);
    }

    /**
     * Forward the message to the next receiver in the ring. If this node is the
     * original sender of the message, then the ring cast is complete.
     * 
     * @param message
     *            - message to forward around the ring
     */
    public void forwardToNextInRing(Message message) {
        if (self.equals(message.sender)) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Ring cast of %s complete on %s",
                                         message, self));
            }
            return;
        }
        ringCast(message);
    }

    /**
     * Forward the message to the next receiver in the ring. If this node is the
     * original sender of the message, then the ring cast is complete.
     * 
     * @param message
     *            - message to forward around the ring
     * @param ring
     *            - the set of nodes forming the ring
     */
    public void forwardToNextInRing(Message message, SortedSet<Node> ring) {
        if (ring.isEmpty() || self.equals(message.sender)) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Ring cast of %s complete on %s",
                                         message, self));
            }
            return;
        }
        ringCast(message, ring);
    }

    public Collection<Node> getDeadMembers() {
        return deadMembers;
    }

    public Node getId() {
        return self;
    }

    public Member getMember() {
        return member;
    }

    public Collection<Node> getMembers() {
        return Collections.unmodifiableSet(members);
    }

    public Collection<Node> getNewMembers() {
        if (previousView == null) {
            return Collections.unmodifiableSet(members);
        }
        TreeSet<Node> newMembers = new TreeSet<Node>();
        for (Node member : members) {
            if (!previousView.contains(member.processId)) {
                newMembers.add(member);
            }
        }
        return newMembers;
    }

    /**
     * @return the state of the reciver. return null if the state is undefined,
     *         such as when the switchboard is transititioning between states
     */
    public SwitchboardState getState() {
        try {
            return fsm.getState();
        } catch (StateUndefinedException e) {
            return null;
        }
    }

    /**
     * Broadcast a message by passing it around the ring formed by the members
     * of the partition
     * 
     * @param message
     *            - the message to pass
     */
    public void ringCast(Message message) {
        assert message != null : "Message must not be null";
        int neighbor = view.rightNeighborOf(self.processId);
        if (neighbor == -1) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Ring does not have right neighbor of %s",
                                         self));
            }
            send(message, self);
        } else {
            send(message, neighbor);
        }
    }

    /**
     * Broadcast a message by passing it around the ring formed by the member
     * set
     * 
     * @param message
     *            - the message to pass
     * @param ring
     *            - the ring of members receiving the message
     */
    public void ringCast(Message message, SortedSet<Node> ring) {
        assert ring != null : "Ring must not be null";

        if (ring.size() <= 1) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Ring does not have right neighbor of %s",
                                         self));
            }
            send(message, self);
        } else {
            send(message, self.getRightNeighbor(ring));
        }
    }

    /**
     * Send a message to a specific node
     * 
     * @param msg
     *            - the message to send
     * @param node
     *            - the receiver of the message
     */
    public void send(final Message msg, final Node node) {
        send(msg, node.processId);
    }

    public void setMember(Member m) {
        member = m;
    }

    @PostConstruct
    public void start() {
        partition.register(notification);
    }

    @PreDestroy
    public void terminate() {
        partition.deregister(notification);
    }

    private void processMessage(final Message message, int sender,
                                final long time) {
        try {
            inboundGate.await();
        } catch (InterruptedException e) {
            return;
        }
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Processing inbound %s on: %s", message,
                                     self));
        }
        message.type.dispatch(Switchboard.this, message.sender,
                              message.arguments, time);
    }

    /**
     * Send a message to a specific node
     * 
     * @param msg
     *            - the message to send
     * @param node
     *            - the process id of the receiver of the message
     */
    private void send(final Message msg, final int node) {
        if (!stable.get()) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Partition is unstable, not sending message %s to %s",
                                       msg, node));
            }
            return;
        }
        if (node == self.processId) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Sending %s to self", msg));
            }
            try {
                try {
                    messageProcessor.execute(new Runnable() {
                        @Override
                        public void run() {
                            processMessage(msg, self.processId,
                                           System.currentTimeMillis());
                        }
                    });
                } catch (RejectedExecutionException e) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest(String.format("rejecting message %s due to shutdown on %s",
                                                 msg, self));
                    }
                }
            } catch (RejectedExecutionException e) {
                if (log.isLoggable(Level.FINEST)) {
                    log.finest(String.format("rejecting message %s due to shutdown on %s",
                                             msg, self));
                }
            }
            return;
        }
        MessageConnection connection = partition.connect(node);
        if (connection == null) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning(String.format("Unable to send %s to %s from %s as the partition cannot create a connection",
                                          msg, node, self));
            }
        } else {
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Sending %s to %s", msg, node));
            }
            connection.sendObject(msg);
        }
    }

    protected void advertise() {
        member.advertise();
    }

    /**
     * Destabilize the partition
     */
    protected void destabilizePartition() {
        inboundGate.close();
        stable.set(false);
        previousMembers.addAll(members);
        deadMembers.clear();
        members.clear();
        member.destabilize();

        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Destabilizing partition on: %s", self));
        }
    }

    /**
     * @return true if this node is the leader
     */
    protected boolean isLeader() {
        return leader;
    }

    /**
     * Stabilize the partition
     * 
     * @param v
     *            - the stable view of the partition
     * @param leader
     *            - the leader
     */
    protected void stabilize(View v, int leader) {
        previousView = view;
        view = v.toBitSet().clone();
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Stabilizing partition on: %s, view: %s, leader: %s",
                                   self, view, leader));
        }
        for (Node member : previousMembers) {
            if (!view.contains(member.processId)) {
                deadMembers.add(member);
            }
        }
        previousMembers.clear();
        stable.set(true);
        inboundGate.open();
    }

    protected void stabilized() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Partition stable and discovery complete on %s",
                                   self));
        }
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("members = %s, new members = %s, dead members = %s on %s",
                                     members, getNewMembers(), deadMembers,
                                     self));
        }
        member.stabilized();
    }

    // test access
    void add(Node node) {
        members.add(node);
    }

    /**
     * Discover the member
     * 
     * @param sender
     *            - the member
     */
    synchronized void discover(Node sender) {
        assert view.contains(sender.processId) : String.format("discovery received from member %s, which is not in the view of %s",
                                                               sender, self);
        if (members.add(sender) && leader
            && members.size() == view.cardinality()) {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("All members discovered on %s", self));
            }
            ringCast(new Message(self, DiscoveryMessage.DISCOVERY_COMPLETE));
        } else {
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("member %s discovered on %s", sender,
                                        self));
            }
        }
    }

    SwitchboardContext getFsm() {
        return fsm;
    }

    /**
     * The partition has changed state.
     * 
     * @param view
     *            - the new view of the partition
     * @param leaderNode
     *            - the elected leader
     */
    void partitionEvent(View view, int leaderNode) {
        leader = self.processId == leaderNode;
        if (view.isStable()) {
            fsm.stabilized(view, leaderNode);
        } else {
            fsm.destabilize();
        }
    }

    /**
     * Establish the cannonical view for this partition. The partition members
     * hold an election to establish which previous view is the "correct" view.
     */
    protected void initiateVoting() {
        ringCast(new Message(self, ViewElectionMessage.VOTE,
                             (Serializable[]) new UUID[] { viewId }));
    }

    protected void establishView(UUID[] votes) {
        HashMap<UUID, Integer> tally = new HashMap<UUID, Integer>();
        for (UUID vote : votes) {
            Integer count = tally.get(vote);
            if (count == null) {
                tally.put(vote, 1);
            } else {
                tally.put(vote, count + 1);
            }
        }
        int i = 0;
        Result[] results = new Result[tally.size()];
        for (Entry<UUID, Integer> entry : tally.entrySet()) {
            results[i++] = new Result(entry.getValue(), entry.getKey());
        }
        Arrays.sort(results);
        Result result = results[results.length - 1];

        UUID newViewId = viewIdGenerator.generate();

        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("View elected: %s on %s new view: %s",
                                   result.vote, self, newViewId));
        }

        ringCast(new Message(self, ViewElectionMessage.NEW_VIEW_ID,
                             new Serializable[] { result.vote, newViewId }));
    }
}
