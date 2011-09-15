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

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.locator.AnubisListener;
import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.locator.AnubisProvider;
import org.smartfrog.services.anubis.locator.AnubisValue;
import org.smartfrog.services.anubis.partition.PartitionManager;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.views.View;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.ContactInformation;

/**
 * This class is a state machine driven by the partitioning events within the
 * group membership as well as by messages sent within the group. This class
 * orchestrates the distributed {@link Coordinator} for the channel buffer
 * process.
 * 
 * @author hhildebrand
 * 
 */
public class Orchestrator {
    class MessageListener extends AnubisListener {
        public MessageListener(String n) {
            super(n);
        }

        @Override
        public void newValue(AnubisValue value) {
            if (value.getValue() == null) {
                return;
            }
            process((Message) value.getValue());
        }

        @Override
        public void removeValue(AnubisValue value) {
        }
    }

    class Notification implements PartitionNotification {

        @Override
        public void objectNotification(Object obj, int sender, long time) {
        }

        @Override
        public void partitionNotification(View view, int leader) {
            partitionEvent(view, leader);
        }

    }

    final static Logger                                   log            = Logger.getLogger(Orchestrator.class.getCanonicalName());

    private volatile int                                  cardinality    = -1;
    private final Coordinator                             coordinator;
    private final List<Node>                              deadMembers    = new CopyOnWriteArrayList<Node>();
    private final AnubisLocator                           locator;
    private final ConcurrentMap<Node, ContactInformation> members        = new ConcurrentHashMap<Node, ContactInformation>();
    private final MessageListener                         msgListener;
    private final AnubisProvider                          msgService;
    private final List<Node>                              newMembers     = new CopyOnWriteArrayList<Node>();
    private final Node                                    node;
    private final PartitionNotification                   notification   = new Notification();
    private final PartitionManager                        partitionManager;
    private final AtomicReference<PartitionState>         partitionState = new AtomicReference<PartitionState>(
                                                                                                               PartitionState.UNSTABLE);
    private final AtomicReference<State>                  state          = new AtomicReference<State>(
                                                                                                      State.INITIAL);

    public Orchestrator(Node n, Coordinator coord, String stateName,
                        AnubisLocator lctr, PartitionManager partitionMgr) {
        node = n;
        coordinator = coord;
        locator = lctr;
        msgService = new AnubisProvider(stateName);
        msgListener = new MessageListener(stateName);
        partitionManager = partitionMgr;
    }

    /**
     * Destabilize the partition
     * 
     * @param view
     *            - the view
     * @param leader
     *            - the leader
     */
    public void destabilize(View view, int leader) {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Destabilizing partition on: %s, view: %s, leader: %s",
                                   node, view, leader));
        }
        deadMembers.clear();
        newMembers.clear();
        for (Entry<Node, ContactInformation> entry : members.entrySet()) {
            Node member = entry.getKey();
            if (!view.contains(member.processId)) {
                deadMembers.add(member);
                members.remove(member);
            }
        }
    }

    /**
     * Receive an introduction from the member
     * 
     * @param member
     *            - the introduced member
     * @param card
     *            - the contact information for the member
     */
    public void introductionFrom(Node member, ContactInformation card) {
        if (members.putIfAbsent(node, card) == null) {
            assert !newMembers.contains(node) : String.format("Already received contact information for: %s",
                                                              member);
            newMembers.add(node);
            if (newMembers.size() == cardinality) {
                transitionTo(State.INTRODUCED);
            }
        }
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
        partitionState.getAndSet(next).next(next, this, view, leader);
    }

    /**
     * Process the inbound message
     * 
     * @param msg
     *            - the message
     */
    public void process(Message msg) {
        final PartitionState ps = partitionState.get();
        switch (ps) {
            case STABLE: {
                if (log.isLoggable(Level.FINE)) {
                    log.fine(String.format("Processing inbound %s on: %s", msg,
                                           node));
                    msg.type.process(msg.body, this);
                }
                break;
            }
            case UNSTABLE: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Discarding %s received during partition instability on: %s",
                                           msg, node));
                }
                break;
            }
            default: {
                log.warning(String.format("%s received, invalid partition state: %s on: %s",
                                          msg, ps, node));
            }
        }
        msg.type.process(msg.body, this);
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
        cardinality = view.cardinality();
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Stabilizing partition on: %s, view: %s, leader: %s",
                                   node, view, leader));
        }
        msgService.setValue(new Message(MessageType.PUBLISH, members.get(node)));
    }

    public void start() {
        locator.registerProvider(msgService);
        locator.registerListener(msgListener);
        partitionManager.register(notification);
    }

    public void terminate() {
        locator.deregisterListener(msgListener);
        locator.deregisterProvider(msgService);
        partitionManager.deregister(notification);
    }

    /**
     * The state machine driver
     * 
     * @param next
     *            - the next state
     */
    private void transitionTo(State next) {
        state.getAndSet(next).next(next, this);
    }
}
