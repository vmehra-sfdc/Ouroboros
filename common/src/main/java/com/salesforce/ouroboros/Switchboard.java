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
                switch (state.get()) {
                    case STABLE: {
                        Message message = (Message) obj;
                        if (log.isLoggable(Level.FINE)) {
                            log.fine(String.format("Processing inbound %s on: %s",
                                                   message, self));
                            message.getType().dispatch(Switchboard.this,
                                                       message.getSender(),
                                                       message.getPayload(),
                                                       time);
                        }
                        break;
                    }
                    case UNSTABLE: {
                        if (log.isLoggable(Level.INFO)) {
                            log.info(String.format("Discarding %s from %s received at %s during partition instability on %s",
                                                   obj, sender, time, self));
                        }
                        break;
                    }
                }
            }
        }

        @Override
        public void partitionNotification(View view, int leader) {
        }
    }

    protected final Logger                        log;
    private final PartitionNotification           notification = new Notification();
    private final Partition                       partition;
    private final AtomicReference<PartitionState> state        = new AtomicReference<PartitionState>(
                                                                                                     PartitionState.UNSTABLE);
    protected final Set<Node>                     deadMembers  = new TreeSet<Node>();
    protected final Set<Node>                     members      = new TreeSet<Node>();
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

    public void destabilize(View view, int leader) {
    }

    abstract public void discoverChannelBuffer(Node from,
                                               ContactInformation payload,
                                               long time);

    public PartitionState getPartitionState() {
        return state.get();
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
            default: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Unable to send %s to %s from %s as the partition is UNSTABLE",
                                           msg, node, self));
                }
            }
        }
    }

    /**
     * Stabilize the partition
     * 
     * @param view
     *            - the stable view of the partition
     * @param leader
     *            - the leader
     */
    public void stabilize(View view, int leader) {
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
    }

    public void start() {
        partition.register(notification);
    }

    public void terminate() {
        partition.deregister(notification);
    }
}
