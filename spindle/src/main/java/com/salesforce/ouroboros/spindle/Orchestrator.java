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

import java.io.Serializable;
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

/**
 * The orchestrator for the channel buffer process.
 * 
 * @author hhildebrand
 * 
 */
public class Orchestrator {
    public static class Message implements Serializable {
        private static final long serialVersionUID = 1L;
        final Serializable        body;
        final MessageType         type;

        Message(MessageType type, Serializable body) {
            this.type = type;
            this.body = body;
        }

        @Override
        public String toString() {
            return "Message [type=" + type + ", body=" + body + "]";
        }
    }

    public static enum MessageType {
        PUBLISH {
            @Override
            void process(Serializable body, Orchestrator orchestrator) {
                Object[] info = (Object[]) body;
                Node n = (Node) info[0];
                ContactInformation card = (ContactInformation) info[1];
                orchestrator.introductionFrom(n, card);
            }
        };

        /**
         * Process the message
         * 
         * @param body
         *            - the body of the message
         * @param orchestrator
         *            - the receiver of the message
         */
        abstract void process(Serializable body, Orchestrator orchestrator);
    }

    public enum PartitionState {
        STABLE {
            @Override
            void next(PartitionState next, Orchestrator orchestrator,
                      View view, int leader) {
                switch (next) {
                    case STABLE: {
                        if (log.isLoggable(Level.INFO)) {
                            log.info(String.format("Stable view received while in the stable state: %s, leader: %s",
                                                   view, leader));
                        }
                        break;
                    }
                    case UNSTABLE: {
                        orchestrator.destabilize(view, leader);
                    }
                }
            }

        },
        UNSTABLE {
            @Override
            void next(PartitionState next, Orchestrator orchestrator,
                      View view, int leader) {
                switch (next) {
                    case UNSTABLE: {
                        if (log.isLoggable(Level.FINEST)) {
                            log.finest(String.format("Untable view received while in the unstable state: %s, leader: %s",
                                                     view, leader));
                        }
                        break;
                    }
                    case STABLE: {
                        orchestrator.stabilize(view, leader);
                    }
                }
            }

        };
        abstract void next(PartitionState next, Orchestrator orchestrator,
                           View view, int leader);
    }

    public enum State {
        INITIAL, REBALANCING
    }

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

    private final static Logger                           log            = Logger.getLogger(Orchestrator.class.getCanonicalName());

    private final List<Node>                              deadMembers    = new CopyOnWriteArrayList<Node>();
    private final AnubisLocator                           locator;
    private final ConcurrentMap<Node, ContactInformation> members        = new ConcurrentHashMap<Node, ContactInformation>();
    private final List<Node>                              newMembers     = new CopyOnWriteArrayList<Node>();
    private final Node                                    node;
    private final PartitionNotification                   notification   = new Notification();
    private final PartitionManager                        partitionManager;
    private final AtomicReference<PartitionState>         partitionState = new AtomicReference<Orchestrator.PartitionState>(
                                                                                                                            PartitionState.UNSTABLE);
    private final AtomicReference<State>                  state          = new AtomicReference<Orchestrator.State>(
                                                                                                                   State.INITIAL);
    private final MessageListener                         stateListener;
    private final AnubisProvider                          stateProvider;

    public Orchestrator(Node node, String stateName, AnubisLocator lctr,
                        PartitionManager partitionMgr) {
        this.node = node;
        locator = lctr;
        stateProvider = new AnubisProvider(stateName);
        stateListener = new MessageListener(stateName);
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
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Stabilizing partition on: %s, view: %s, leader: %s",
                                   node, view, leader));
        }
    }

    public void start() {
        locator.registerProvider(stateProvider);
        locator.registerListener(stateListener);
        partitionManager.register(notification);
    }

    public void terminate() {
        locator.deregisterListener(stateListener);
        locator.deregisterProvider(stateProvider);
        partitionManager.deregister(notification);
    }
}
