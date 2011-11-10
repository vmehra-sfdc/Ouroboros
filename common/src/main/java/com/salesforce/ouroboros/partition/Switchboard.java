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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.comms.MessageConnection;
import org.smartfrog.services.anubis.partition.util.NodeIdSet;
import org.smartfrog.services.anubis.partition.views.View;

import com.salesforce.ouroboros.ChannelMessage;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardFSM;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardState;

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
         * The partition has been destabilized
         */
        void destabilize();

        void dispatch(ChannelMessage type, Node sender, Serializable payload,
                      long time);

        void dispatch(GlobalMessageType type, Node sender,
                      Serializable payload, long time);

        void dispatch(MemberDispatch type, Node sender, Serializable payload,
                      long time);

        void stabilized();

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
                            processMessage((Message) obj, sender, time);
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

        private void processMessage(final Message message, int sender,
                                    final long time) {
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Processing inbound %s on: %s", message,
                                       self));
            }
            message.type.dispatch(Switchboard.this, message.sender,
                                  message.payload, time);
        }
    }

    static final Logger                 log             = Logger.getLogger(Switchboard.class.getCanonicalName());
    private final SortedSet<Node>       deadMembers     = new TreeSet<Node>();
    private final SwitchboardContext    fsm             = new SwitchboardContext(
                                                                                 this);
    private boolean                     leader          = false;
    private SortedSet<Node>             members         = new ConcurrentSkipListSet<Node>();
    private final Executor              messageProcessor;
    private final PartitionNotification notification    = new Notification();
    private final Partition             partition;
    private SortedSet<Node>             previousMembers = new ConcurrentSkipListSet<Node>();
    private NodeIdSet                   previousView;
    private final Node                  self;
    protected NodeIdSet                 view;
    Member                              member;

    public Switchboard(Node node, Partition p) {
        self = node;
        partition = p;
        messageProcessor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread daemon = new Thread(
                                           r,
                                           String.format("Message processor for <%s>",
                                                         member));
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.log(Level.SEVERE,
                                "Uncaught exception on message processing thread",
                                e);
                    }
                });
                return daemon;
            }
        });
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

    public void dispatchToMember(ChannelMessage type, Node sender,
                                 Serializable payload, long time) {
        member.dispatch(type, sender, payload, time);
    }

    public void dispatchToMember(MemberDispatch type, Node sender,
                                 Serializable payload, long time) {
        member.dispatch(type, sender, payload, time);
    }

    public Collection<Node> getDeadMembers() {
        return deadMembers;
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

    public SwitchboardState getState() {
        return fsm.getState();
    }

    public boolean isStable() {
        return fsm.getState() == SwitchboardFSM.Stable;
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
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Ring does not have right neighbor of %s",
                                       self));
            }
        }
        MessageConnection connection = partition.connect(neighbor);
        if (connection == null) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning(String.format("Unable to send %s to %s from %s as the partition cannot create a connection",
                                          message, neighbor, self));
            }
        } else {
            connection.sendObject(message);
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
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Ring does not have right neighbor of %s",
                                       self));
            }
            return;
        }
        send(message, getRightNeighbor(ring));
    }

    /**
     * Send a message to a specific node
     * 
     * @param msg
     *            - the message to send
     * @param node
     *            - the receiver of the message
     */
    public void send(Message msg, Node node) {
        MessageConnection connection = partition.connect(node.processId);
        if (connection == null) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning(String.format("Unable to send %s to %s from %s as the partition cannot create a connection",
                                          msg, node, self));
            }
        } else {
            connection.sendObject(msg);
        }
    }

    public void setMember(Member m) {
        member = m;
    }

    public void start() {
        partition.register(notification);
    }

    public void terminate() {
        partition.deregister(notification);
    }

    private Node getRightNeighbor(SortedSet<Node> ring) {
        Iterator<Node> tail = ring.tailSet(self).iterator();
        tail.next();
        return tail.hasNext() ? tail.next() : ring.first();
    }

    protected void advertise() {
        member.advertise();
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
    }

    protected void stabilized() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Partition stable and discovery complete on %s",
                                   self));
        }
        member.stabilized();
    }

    // test access
    void add(Node node) {
        members.add(node);
    }

    /**
     * Destabilize the partition
     * 
     * @param view
     *            - the view
     * @param leader
     *            - the leader
     */
    void destabilize(View view, int leader) {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Destabilizing partition on: %s, view: %s, leader: %s",
                                   self, view, leader));
        }
        previousMembers.addAll(members);
        deadMembers.clear();
        members.clear();
        member.destabilize();
    }

    /**
     * Discover the member
     * 
     * @param sender
     *            - the member
     */
    synchronized void discover(Node sender) {
        if (!view.contains(sender.processId)) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning(String.format("discovery received from member %s, which is not in the view of %s",
                                          sender, self));
            }
            return;
        }
        if (leader && members.add(sender)
            && members.size() == view.cardinality()) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("All members discovered on %s", self));
            }
            ringCast(new Message(self, GlobalMessageType.DISCOVERY_COMPLETE));
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("member %s discovered on %s", sender,
                                       self));
            }
        }
    }

    /**
     * The double dispatch of the GlobalMessage
     * 
     * @param type
     * @param sender
     * @param payload
     * @param time
     */
    void dispatch(GlobalMessageType type, Node sender, Serializable payload,
                  long time) {
        if (self.equals(sender)) {
            if (log.isLoggable(Level.FINER)) {
                log.fine(String.format("Complete ring traversal of %s from %s",
                                       type, self));
            }
        } else {
            ringCast(new Message(sender, type, payload));
        }
        switch (type) {
            case DISCOVERY_COMPLETE:
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Discovery complete on %s", self));
                }
                fsm.discoveryComplete();
                break;
            default:
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Discovery of node %s = %s", self,
                                           type));
                }
                discover(sender);
                member.dispatch(type, sender, payload, time);
                break;
        }
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
            fsm.destabilize(view, leaderNode);
        }
    }
}
