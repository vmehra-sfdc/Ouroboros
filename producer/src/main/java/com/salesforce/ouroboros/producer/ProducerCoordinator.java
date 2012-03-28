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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;
import com.salesforce.ouroboros.producer.Producer.UpdateState;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext.ProducerCoordinatorState;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the producer node.
 * 
 * @author hhildebrand
 * 
 */
public class ProducerCoordinator implements Member {
    private static enum Pending {
        MIRROR_OPENED, PENDING, PRIMARY_OPENED
    };

    private final static Logger                 log                    = Logger.getLogger(ProducerCoordinator.class.getCanonicalName());

    private boolean                             active                 = false;
    private final SortedSet<Node>               activeProducers        = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               activeWeavers          = new ConcurrentSkipListSet<Node>();
    private final ProducerCoordinatorContext    fsm                    = new ProducerCoordinatorContext(
                                                                                                        this);
    private final SortedSet<Node>               inactiveMembers        = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               inactiveWeavers        = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>               joiningWeavers         = new ConcurrentSkipListSet<Node>();
    private Node[]                              joiningProducers       = new Node[0];
    private final SortedSet<Node>               nextProducerMembership = new ConcurrentSkipListSet<Node>();
    private final ConcurrentMap<UUID, Pending>  pendingChannels        = new ConcurrentHashMap<UUID, ProducerCoordinator.Pending>();
    private final Producer                      producer;
    private final List<UpdateState>             rebalanceUpdates       = new ArrayList<Producer.UpdateState>();
    private final Node                          self;
    private final Switchboard                   switchboard;
    private final AtomicInteger                 tally                  = new AtomicInteger();
    private final Map<Node, ContactInformation> yellowPages            = new ConcurrentHashMap<Node, ContactInformation>();

    public ProducerCoordinator(Switchboard switchboard, Producer producer)
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
        producer.inactivate();
        active = false;
    }

    @Override
    public void destabilize() {
        fsm.destabilize();
    }

    @Override
    public void dispatch(BootstrapMessage type, Node sender,
                         Serializable[] arguments, long time) {
        Node[] nodes = (Node[]) arguments[0];
        switch (type) {
            case BOOTSTRAP_PRODUCERS: {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Bootstrapping producers on: %s",
                                           self));
                }
                ConsistentHashFunction<Node> ring = producer.createRing();
                for (Node node : nodes) {
                    ring.add(node, node.capacity);
                    inactiveMembers.remove(node);
                    activeProducers.add(node);
                }
                producer.setProducerRing(ring);
                active = true;
                fsm.bootstrapped();
                break;
            }
            case BOOTSTRAP_SPINDLES: {
                ConsistentHashFunction<Node> ring = producer.createRing();
                for (Node node : nodes) {
                    ring.add(node, node.capacity);
                    inactiveWeavers.remove(node);
                    activeWeavers.add(node);
                }
                producer.createSpinners(activeWeavers, yellowPages);
                producer.remapWeavers(ring);
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
            case OPEN: {
                UUID channel = (UUID) arguments[0];
                if (producer.isResponsibleFor(channel)) {
                    pendingChannels.put(channel, Pending.PENDING);
                }
                break;
            }
            case CLOSE: {
                break;
            }
            case PRIMARY_OPENED: {
                UUID channel = (UUID) arguments[0];
                if (producer.isResponsibleFor(channel)) {
                    Pending state = pendingChannels.get(channel);
                    assert state != null : String.format("No pending state for %s on: %s",
                                                         channel, self);
                    switch (state) {
                        case PENDING:
                            pendingChannels.put(channel, Pending.PRIMARY_OPENED);
                            break;
                        case PRIMARY_OPENED:
                            log.severe(String.format("%s already believes that the channel %s has been opened by the primary",
                                                     self, channel));
                            break;
                        case MIRROR_OPENED:
                            pendingChannels.remove(channel);
                            producer.opened(channel);
                            break;
                        default:
                            throw new IllegalStateException(
                                                            String.format("Invalid channel message: %s",
                                                                          type));
                    }
                }
                break;
            }
            case MIRROR_OPENED: {
                UUID channel = (UUID) arguments[0];
                if (producer.isResponsibleFor(channel)) {
                    Pending state = pendingChannels.get(channel);
                    assert state != null : String.format("No pending state for %s on: %s",
                                                         channel, self);
                    switch (state) {
                        case PENDING:
                            pendingChannels.put(channel, Pending.MIRROR_OPENED);
                            break;
                        case PRIMARY_OPENED:
                            pendingChannels.remove(channel);
                            producer.opened(channel);
                            break;
                        case MIRROR_OPENED:
                            log.severe(String.format("%s already believes that the channel %s has been opened by the mirror",
                                                     self, channel));
                            break;
                        default:
                            throw new IllegalStateException(
                                                            String.format("Invalid channel message: %s",
                                                                          type));
                    }
                }
                break;
            }
            case PAUSE_CHANNELS: {
                ArrayList<UUID> channels = new ArrayList<UUID>(arguments.length);
                for (Serializable s : arguments) {
                    channels.add((UUID) s);
                }
                producer.pause(channels);
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
                    activeProducers.add(sender);
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
                if (active) {
                    failover();
                }
                break;
            default: {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid failover message: %s",
                                              type));
                }
            }
        }
    }

    public void dispatch(ProducerRebalanceMessage type, Node sender,
                         Serializable[] arguments, long time,
                         Switchboard switchboard2) {
        switch (type) {
            case PREPARE_FOR_REBALANCE:
                rebalance((Node[]) arguments[0]);
                break;
            case INITIATE_REBALANCE:
                rebalance();
                break;
            case MEMBER_REBALANCED: {
                if (isActiveLeader()) {
                    log.info(String.format("%s marked as rebalanced on %s",
                                           sender, self));
                    tally.decrementAndGet();
                    fsm.memberRebalanced();
                } else {
                    switchboard.forwardToNextInRing(new Message(sender, type,
                                                                arguments),
                                                    nextProducerMembership);
                }
                break;
            }
            case TAKEOVER:
                rebalanced();
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Invalid rebalance message %s",
                                                              type));
        }
    }

    public void dispatch(UpdateMessage updateMessage, Node sender,
                         Serializable[] arguments, long time,
                         Switchboard switchboard2) {
        Producer.UpdateState[] packet = (UpdateState[]) arguments[1];
        for (Producer.UpdateState update : packet) {
            rebalanceUpdates.add(update);
        }
    }

    @Override
    public void dispatch(WeaverRebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PREPARE_FOR_REBALANCE: {
                for (Node n : (Node[]) arguments[0]) {
                    joiningWeavers.add(n);
                }
                break;
            }
            case REBALANCE_COMPLETE: {
                ConsistentHashFunction<Node> ring = producer.createRing();
                for (Node node : joiningWeavers) {
                    inactiveWeavers.remove(node);
                    activeWeavers.add(node);
                }
                for (Node node: activeWeavers) { 
                    ring.add(node, node.capacity);
                }
                producer.createSpinners(joiningWeavers, yellowPages);
                producer.remapWeavers(ring);
                producer.resumePausedChannels();
                joiningWeavers.clear();
                break;
            }
            case INITIATE_REBALANCE:
                break;
            case TAKEOVER:
                break;
            default:
                throw new IllegalStateException(
                                                String.format("Unknown rebalance message: %s",
                                                              type));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof ProducerCoordinator) {
            return self.equals(((ProducerCoordinator) o).self);
        }
        return false;
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
     * @return
     */
    public Producer getProducer() {
        return producer;
    }

    /**
     * @return the state of the reciver. return null if the state is undefined,
     *         such as when the coordinator is transititioning between states
     */
    public ProducerCoordinatorState getState() {
        try {
            return fsm.getState();
        } catch (StateUndefinedException e) {
            return null;
        }
    }

    @Override
    public int hashCode() {
        return self.hashCode();
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
        if (!isInactiveLeader()) {
            throw new IllegalStateException(
                                            "This node must be inactive and the leader to initiate rebalancing");
        }
        if (activeProducers.size() != 0) {
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
     * Initiate the rebalancing of the producer ring using the set of inactive
     * members
     */
    public void initiateRebalance() {
        initiateRebalance(inactiveMembers.toArray(new Node[inactiveMembers.size()]));
    }

    /**
     * Initiate the rebalancing of the producer ring.
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

    public boolean isActive() {
        return active;
    }

    /**
     * Answer true if the receiver is active and the leader of the active group
     * 
     * @return
     */
    public boolean isActiveLeader() {
        if (active) {
            return activeProducers.isEmpty() ? true
                                            : activeProducers.last().equals(self);
        }
        return false;
    }

    @Override
    public void stabilized() {
        filterSystemMembership();
        fsm.stabilize();
    }

    @Override
    public String toString() {
        return String.format("Coordinator for producer [%s]", self.processId);
    }

    private void calculateNextProducerRing() {
        ConsistentHashFunction<Node> newRing = producer.createRing();
        for (Node node : activeProducers) {
            Node clone = node.clone();
            newRing.add(clone, clone.capacity);
        }
        for (Node node : joiningProducers) {
            Node clone = node.clone();
            newRing.add(clone, clone.capacity);
        }
        producer.setNextProducerRing(newRing);
    }

    protected void beginRebalance(Node[] joiningMembers) {
        setJoiningProducers(joiningMembers);
    }

    /**
     * Clean up any state when destabilizing the partition.
     */
    protected void cleanUp() {
        joiningProducers = new Node[0];
        joiningWeavers.clear();
        pendingChannels.clear();
        nextProducerMembership.clear();
        tally.set(0);
        rebalanceUpdates.clear();
        producer.cleanUp();
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
                                         BootstrapMessage.BOOTSTRAP_PRODUCERS,
                                         (Serializable) joiningProducers));
    }

    /**
     * The receiver is the controller for the group. Coordinate the rebalancing
     * of the system by including the new members.
     */
    protected void coordinateRebalance() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Coordinating rebalancing on %s", self));
        }
        tally.set(nextProducerMembership.size());
        switchboard.ringCast(new Message(
                                         self,
                                         ProducerRebalanceMessage.PREPARE_FOR_REBALANCE,
                                         (Serializable) joiningProducers),
                             nextProducerMembership);
    }

    /**
     * Coordinate the takeover of the completion of the rebalancing
     */
    protected void coordinateTakeover() {
        switchboard.ringCast(new Message(self,
                                         ProducerRebalanceMessage.TAKEOVER),
                             nextProducerMembership);
    }

    protected void destabilizePartition() {
        switchboard.destabilize();
    }

    /**
     * Failover the process, assuming primary role for any failed primaries this
     * process is serving as the mirror
     */
    protected void failover() {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Initiating failover on %s", self));
        }
        try {
            producer.failover(switchboard.getDeadMembers());
        } catch (InterruptedException e) {
            return;
        }
        fsm.failedOver();
    }

    /**
     * Remove all dead members and partition out the new members from the
     * members that were part of the previous partition
     */
    protected void filterSystemMembership() {
        activeProducers.removeAll(switchboard.getDeadMembers());
        activeWeavers.removeAll(switchboard.getDeadMembers());
        inactiveMembers.removeAll(switchboard.getDeadMembers());
        inactiveWeavers.removeAll(switchboard.getDeadMembers());
        nextProducerMembership.clear();
        nextProducerMembership.addAll(activeProducers);
    }

    protected SortedSet<Node> getActiveProducers() {
        return activeProducers;
    }

    protected SortedSet<Node> getNextProducerMembership() {
        return nextProducerMembership;
    }

    protected boolean hasActiveMembers() {
        return !activeProducers.isEmpty();
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

    protected void openChannel(UUID channel) {
        // TODO Auto-generated method stub

    }

    protected void rebalance() {
        rebalance(producer.remap());
    }

    /**
     * Rebalance the channels which this node has responsibility for
     * 
     * @param remapped
     *            - the mapping of channels to their new primary/mirror pairs
     */
    protected void rebalance(Map<UUID, Node[][]> remappping) {
        HashMap<Node, List<UpdateState>> remapped = new HashMap<Node, List<UpdateState>>();
        for (Entry<UUID, Node[][]> entry : remappping.entrySet()) {
            producer.rebalance(remapped, entry.getKey(),
                               entry.getValue()[0][0], entry.getValue()[0][1],
                               entry.getValue()[1][0], entry.getValue()[1][1],
                               activeProducers);
        }

        for (Map.Entry<Node, List<Producer.UpdateState>> entry : remapped.entrySet()) {
            List<UpdateState> updates = entry.getValue();
            int delta = 20;
            for (int i = 0; i < updates.size();) {
                List<UpdateState> packet = updates.subList(i,
                                                           Math.min(i + delta,
                                                                    updates.size()));
                i += delta;
                switchboard.ringCast(new Message(
                                                 self,
                                                 UpdateMessage.UPDATE,
                                                 new Serializable[] {
                                                         entry.getKey(),
                                                         packet.toArray(new Producer.UpdateState[packet.size()]) }),
                                     nextProducerMembership);
            }
        }

        fsm.rebalanced();
        switchboard.ringCast(new Message(
                                         self,
                                         ProducerRebalanceMessage.MEMBER_REBALANCED),
                             nextProducerMembership);
    }

    /**
     * Calculate the rebalancing of the system using the supplied list of
     * joining producer processes.
     * 
     * @param joiningMembers
     *            - the list of producers that are joining the process group
     */
    protected void rebalance(Node[] joiningMembers) {
        setJoiningProducers(joiningMembers);
        calculateNextProducerRing();
        fsm.rebalance();
    }

    /**
     * The producer cluster has been rebalanced. Switch over to the new
     * membership and commit the takeover
     */
    protected void rebalanced() {
        activeProducers.clear();
        activeProducers.addAll(nextProducerMembership);
        inactiveMembers.removeAll(nextProducerMembership);
        joiningProducers = new Node[0];
        producer.createSpinners(activeWeavers, yellowPages);
        producer.commitProducerRing(rebalanceUpdates);
        rebalanceUpdates.clear();
        nextProducerMembership.clear();
        active = true;
        fsm.commitTakeover();
    }

    protected void rebalancePrepared() {
        switchboard.ringCast(new Message(
                                         self,
                                         ProducerRebalanceMessage.INITIATE_REBALANCE),
                             nextProducerMembership);
    }

    /**
     * Set the joining members of the receiver
     * 
     * @param joiningMembers
     */
    protected void setJoiningProducers(Node[] joiningMembers) {
        assert joiningMembers != null : "joining members must not be null";
        joiningProducers = joiningMembers;
        for (Node node : joiningMembers) {
            nextProducerMembership.add(node);
        }
    }

    /**
     * @return true if the tally is equal to the required size
     */
    protected boolean tallyComplete() {
        return tally.get() == 0;
    }
}
