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
package com.salesforce.ouroboros.producer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import com.salesforce.ouroboros.producer.CoordinatorContext.CoordinatorState;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the producer node.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {

    private final static Logger                 log             = Logger.getLogger(Coordinator.class.getCanonicalName());

    private boolean                             active          = false;
    private final SortedSet<Node>               activeMembers   = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               activeWeavers   = new ConcurrentSkipListSet<Node>();
    private final CoordinatorContext            fsm             = new CoordinatorContext(
                                                                                         this);
    private final SortedSet<Node>               inactiveMembers = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               inactiveWeavers = new ConcurrentSkipListSet<Node>();
    private Node[]                              joiningMembers  = new Node[0];
    @SuppressWarnings("unused")
    private Node[]                              joiningWeavers  = new Node[0];
    @SuppressWarnings("unused")
    private ConsistentHashFunction<Node>        nextWeaverRing;
    private final Producer                      producer;
    private final Node                          self;
    private final Switchboard                   switchboard;

    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(Switchboard switchboard, Producer producer)
                                                                  throws IOException {
        this.producer = producer;
        self = producer.getId();
        fsm.setName(Integer.toString(self.processId));
        switchboard.setMember(this);
        this.switchboard = switchboard;
    }

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(self,
                                         DiscoveryMessage.ADVERTISE_PRODUCER,
                                         active));
    }

    @Override
    public void becomeInactive() {
        active = false;
    }

    @Override
    public void destabilize() {
        fsm.destabilize();
    }

    @Override
    public void dispatch(BootstrapMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case BOOTSTAP_PRODUCERS: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Bootstrapping producers on: %s",
                                           self));
                }
                ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
                for (Node node : (Node[]) arguments[0]) {
                    ring.add(node, node.capacity);
                    inactiveMembers.remove(node);
                    activeMembers.add(node);
                }
                producer.setProducerRing(ring);
                active = true;
                switchboard.forwardToNextInRing(new Message(sender, type,
                                                            arguments));
                fsm.bootstrapped();
                break;
            }
            case BOOTSTRAP_SPINDLES: {
                ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
                for (Node node : (Node[]) arguments[0]) {
                    ring.add(node, node.capacity);
                    inactiveWeavers.remove(node);
                    activeWeavers.add(node);
                }
                producer.remapWeavers(ring);
                switchboard.forwardToNextInRing(new Message(sender, type,
                                                            arguments));
                break;
            }
            default:
                throw new IllegalStateException(
                                                String.format("Invalid bootstrap message: %s",
                                                              type));
        }
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PRIMARY_OPENED: {
                break;
            }
            case MIRROR_OPENED: {
                break;
            }
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
                    activeWeavers.add(sender);
                } else {
                    inactiveWeavers.add(sender);
                }
                yellowPages.put(sender, (ContactInformation) arguments[0]);
                break;
            case ADVERTISE_PRODUCER:
                if ((Boolean) arguments[0]) {
                    activeMembers.add(sender);
                } else {
                    inactiveMembers.add(sender);
                }
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
                break;
            case FAILOVER:
                failover();
                break;
            default: {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid failover message: %s",
                                              type));
                }
            }
        }
    }

    @Override
    public void dispatch(RebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case INITIATE_REBALANCE:
            case PREPARE_FOR_REBALANCE:
            case REBALANCE_COMPLETE:
            default:
                throw new IllegalStateException(
                                                String.format("Unknown rebalance message: %s",
                                                              type));
        }
    }

    /**
     * Answer the node represting this coordinator's id
     * 
     * @return the Node representing this process
     */
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
     * Initiate the bootstrapping of the producer ring using the set of inactive
     * members
     */
    public void initiateBootstrap() {
        initiateBootstrap(inactiveMembers.toArray(new Node[inactiveMembers.size()]));
    }

    /**
     * Initiate the bootstrapping of the producer ring
     * 
     * @param joiningMembers
     *            - the bootstrap membership set
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

    @Override
    public void stabilized() {
        fsm.stabilize();
    }

    @Override
    public String toString() {
        return String.format("Coordinator for producer [%s]", self.processId);
    }

    /**
     * Remove all dead members and partition out the new members from the
     * members that were part of the previous partition
     */
    private void filterSystemMembership() {
        activeMembers.removeAll(switchboard.getDeadMembers());
        activeWeavers.removeAll(switchboard.getDeadMembers());
        inactiveMembers.removeAll(switchboard.getDeadMembers());
        inactiveWeavers.removeAll(switchboard.getDeadMembers());
    }

    /**
     * Clean up any state when destabilizing the partition.
     */
    protected void cleanUp() {
        joiningMembers = joiningWeavers = new Node[0];
    }

    /**
     * Coordinate the bootstrapping of the producer process group.
     */
    protected void coordinateBootstrap() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating producer bootstrap on %s",
                                   self));
        }
        switchboard.ringCast(new Message(self,
                                         BootstrapMessage.BOOTSTAP_PRODUCERS,
                                         (Serializable) joiningMembers));
    }

    /**
     * Failover the process, assuming primary role for any failed primaries this
     * process is serving as the mirror
     */
    protected void failover() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Initiating failover on %s", self));
        }
        filterSystemMembership();
        fsm.failedOver();
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
                                            : activeMembers.last().equals(self);
        }
        return inactiveMembers.size() == 0 ? true
                                          : inactiveMembers.last().equals(self);
    }

    protected void openChannel(UUID channel) {
        // TODO Auto-generated method stub

    }

    protected void setJoiningMembers(Node[] joiningMembers) {
        this.joiningMembers = joiningMembers;
    }
}
