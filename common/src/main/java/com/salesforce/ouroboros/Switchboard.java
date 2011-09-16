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
package com.salesforce.ouroboros;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.comms.MessageConnection;
import org.smartfrog.services.anubis.partition.util.NodeIdSet;
import org.smartfrog.services.anubis.partition.views.View;

/**
 * The common high level distribute coordination logic for Ouroboros group
 * members. Provides the basic handling for handling partitions, maintaining
 * group membership, accounting for dead members after a partition change, and
 * the message passing logic.
 * 
 * @author hhildebrand
 * 
 */
abstract public class Switchboard {

    class Notification implements PartitionNotification {

        @Override
        public void objectNotification(Object obj, int sender, long time) {
            if (obj instanceof Message) {
                Message message = (Message) obj;
                processMessage(message, sender, time);
            } else if (obj instanceof RingMessage) {
                RingMessage message = (RingMessage) obj;
                processRingMessage(message, sender, time);
            }
        }

        @Override
        public void partitionNotification(View view, int leader) {
        }

        private void forward(RingMessage message) {
            int neighbor = view.leftNeighborOf(self.processId);
            if (neighbor == -1) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Unable to ring cast %s from %s on: %s to: %s there is no left neighbor",
                                           message.wrapped, message.from, self,
                                           neighbor));
                }
                return;
            }
            if (neighbor == message.from) {
                if (log.isLoggable(Level.FINEST)) {
                    log.fine(String.format("Ring message %s completed traversal from %s",
                                           message.wrapped, self));
                }
                return;
            }
            MessageConnection connection = partition.connect(neighbor);
            if (connection == null) {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Unable to ring cast %s from %s on: %s to %s as the partition cannot create a connection",
                                              message.wrapped, message.from,
                                              self, neighbor));
                }
            } else {
                connection.sendObject(message);
            }
        }

        private void processMessage(Message message, int sender, long time) {
            switch (state.get()) {
                case STABLE: {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine(String.format("Processing inbound %s on: %s",
                                               message, self));
                    }
                    message.getType().dispatch(Switchboard.this,
                                               message.getSender(),
                                               message.getPayload(), time);
                    break;
                }
                case UNSTABLE: {
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Discarding %s from %s received at %s during partition instability on %s",
                                               message, sender, time, self));
                    }
                    break;
                }
            }
        }

        private void processRingMessage(RingMessage message, int sender,
                                        long time) {
            Message wrapped = message.wrapped;
            switch (state.get()) {
                case STABLE: {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine(String.format("Processing inbound ring message %s on: %s",
                                               wrapped, self));
                    }
                    forward(message);
                    wrapped.getType().dispatch(Switchboard.this,
                                               wrapped.getSender(),
                                               wrapped.getPayload(), time);
                    break;
                }
                case UNSTABLE: {
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Discarding %s from %s received at %s during partition instability on %s",
                                               wrapped, sender, time, self));
                    }
                    break;
                }
            }
        }
    }

    private final Set<Node>                       deadMembers  = new TreeSet<Node>();
    private boolean                               leader       = false;
    private final Set<Node>                       members      = new TreeSet<Node>();
    private final Set<Node>                       newMembers   = new TreeSet<Node>();
    private final PartitionNotification           notification = new Notification();
    private final Partition                       partition;
    private final AtomicReference<PartitionState> state        = new AtomicReference<PartitionState>(
                                                                                                     PartitionState.UNSTABLE);
    private NodeIdSet                             view;
    protected final Logger                        log;
    protected final Node                          self;

    public Switchboard(Node node, Partition p) {
        self = node;
        partition = p;
        log = Logger.getLogger(getClass().getCanonicalName());
    }

    public void add(Node node) {
        members.add(node);
    }

    /**
     * Advertise the receiver service, by ring casting the service across the
     * membership ring
     */
    abstract public void advertise();

    /**
     * Broadcast the message to all the members in our partition
     * 
     * @param msg
     *            - the message to broadcast
     */
    public void broadcast(Message msg) {
        for (Node node : members) {
            send(msg, node);
        }
    }

    /**
     * The partition has been destabilized
     */
    abstract public void destabilize();

    abstract public void discoverChannelBuffer(Node from,
                                               ContactInformation payload,
                                               long time);

    public PartitionState getPartitionState() {
        return state.get();
    }

    /**
     * Membership discover is now complete
     */
    public void membershipComplete() {
        // TODO Auto-generated method stub

    }

    /**
     * The partition has changed state.
     * 
     * @param view
     *            - the new view of the partition
     * @param leader
     *            - the elected leader
     */
    public void partitionEvent(View view, int leader) {
        PartitionState next = view.isStable() ? PartitionState.STABLE
                                             : PartitionState.UNSTABLE;
        state.getAndSet(next).next(next, this, view, leader);
    }

    public void remove(Node node) {
        members.remove(node);
    }

    /**
     * Broadcast a message by passing it around the ring formed by the members
     * of the partition
     * 
     * @param message
     *            - the message to pass
     */
    public void ringCast(Message message) {
        switch (state.get()) {
            case STABLE: {
                int neighbor = view.leftNeighborOf(self.processId);
                if (neighbor == -1) {
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Unable to ring cast %s from %s as there is no left neighbor",
                                               message, self));
                    }
                    return;
                }
                MessageConnection connection = partition.connect(neighbor);
                if (connection == null) {
                    if (log.isLoggable(Level.WARNING)) {
                        log.warning(String.format("Unable to ring cast %s from %s as the partition cannot create a connection",
                                                  message, self));
                    }
                } else {
                    connection.sendObject(new RingMessage(self.processId,
                                                          message));
                }

                break;
            }
            case UNSTABLE: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Unable to ring cast %s from %s as the partition is UNSTABLE",
                                           message, self));
                }
            }
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
    public void send(Message msg, Node node) {
        switch (state.get()) {
            case STABLE: {
                MessageConnection connection = partition.connect(node.processId);
                if (connection == null) {
                    if (log.isLoggable(Level.WARNING)) {
                        log.warning(String.format("Unable to send %s to %s from %s as the partition cannot create a connection",
                                                  msg, node, self));
                    }
                } else {
                    connection.sendObject(msg);
                }
                break;
            }
            case UNSTABLE: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Unable to send %s to %s from %s as the partition is UNSTABLE",
                                           msg, node, self));
                }
            }
        }
    }

    public void start() {
        partition.register(notification);
    }

    public void terminate() {
        partition.deregister(notification);
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
        destabilize();
        deadMembers.clear();
        newMembers.clear();
    }

    void discover(Node sender) {
        if (members.add(sender)) {
            if (members.size() + newMembers.size() == view.cardinality()) {
                membershipComplete();
            }
        }
    }

    /**
     * Stabilize the partition
     * 
     * @param v
     *            - the stable view of the partition
     * @param leader
     *            - the leader
     */
    void stabilize(View v, int leader) {
        view = v.toBitSet();
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Stabilizing partition on: %s, view: %s, leader: %s",
                                   self, view, leader));
        }
        for (Node member : members) {
            if (!view.contains(member.processId)) {
                deadMembers.add(member);
                members.remove(member);
            }
        }
        advertise();
    }
}
