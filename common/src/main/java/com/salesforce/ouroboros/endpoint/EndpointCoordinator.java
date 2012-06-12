/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.endpoint;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statemap.StateUndefinedException;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.endpoint.EndpointCoordinatorContext.EndpointCoordinatorState;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * @author hhildebrand
 * 
 */
abstract public class EndpointCoordinator implements Member {

    private static Logger                         log             = LoggerFactory.getLogger(EndpointCoordinator.class);

    protected boolean                             active          = false;
    protected final SortedSet<Node>               activeMembers   = new ConcurrentSkipListSet<Node>();
    protected final SortedSet<Node>               activeWeavers   = new ConcurrentSkipListSet<Node>();
    protected final SortedSet<Node>               inactiveMembers = new ConcurrentSkipListSet<Node>();
    protected final SortedSet<Node>               inactiveWeavers = new ConcurrentSkipListSet<Node>();
    protected Node[]                              joiningMembers  = new Node[0];
    protected final SortedSet<Node>               joiningWeavers  = new ConcurrentSkipListSet<Node>();
    protected final SortedSet<Node>               nextMembership  = new ConcurrentSkipListSet<Node>();
    protected final Node                          self;
    protected final Switchboard                   switchboard;
    private final AtomicInteger                   tally           = new AtomicInteger();
    protected final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();
    protected final EndpointCoordinatorContext    fsm             = new EndpointCoordinatorContext(
                                                                                                   this);

    public EndpointCoordinator(Switchboard switchboard, Node self) {
        this.self = self;
        switchboard.setMember(this);
        this.switchboard = switchboard;
    }

    @Override
    public void becomeInactive() {
        active = false;
    }

    public void beginRebalance(Node[] joiningMembers) {
        setJoiningMembers(joiningMembers);
    }

    @Override
    public void destabilize() {
        fsm.destabilize();
    }

    public void destabilizePartition() {
        switchboard.destabilize();
    }

    public void dispatch(EndpointRebalanceMessage type, Node sender,
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
                                                    nextMembership);
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
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Invalid failover message: %s", type));
                }
            }
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
                for (Node node : joiningWeavers) {
                    inactiveWeavers.remove(node);
                    activeWeavers.add(node);
                }
                rebalanceComplete();
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
        if (o instanceof EndpointCoordinator) {
            return self.equals(((EndpointCoordinator) o).self);
        }
        return false;
    }

    public SortedSet<Node> getActiveMembers() {
        return activeMembers;
    }

    /**
     * Answer the node represting this coordinator's id
     * 
     * @return the Node representing this process
     */
    public Node getId() {
        return self;
    }

    public SortedSet<Node> getNextMembership() {
        return nextMembership;
    }

    /**
     * @return the state of the reciver. return null if the state is undefined,
     *         such as when the coordinator is transititioning between states
     */
    public EndpointCoordinatorState getState() {
        try {
            return fsm.getState();
        } catch (StateUndefinedException e) {
            return null;
        }
    }

    public boolean hasActiveMembers() {
        return !activeMembers.isEmpty();
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
        bootstrapSystem(joiningMembers);
    }

    /**
     * Initiate the rebalancing of the endpoint ring using the set of inactive
     * members
     */
    public void initiateRebalance() {
        initiateRebalance(inactiveMembers.toArray(new Node[inactiveMembers.size()]));
    }

    /**
     * Initiate the rebalancing of the endpoint's ring.
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
        initialiateRebalancing(joiningMembers);
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
            return activeMembers.isEmpty() ? true
                                          : activeMembers.last().equals(self);
        }
        return false;
    }

    /**
     * Answer true if the receiver is not active and the leader of the inactive
     * group
     * 
     * @return
     */
    public boolean isInactiveLeader() {
        if (!active) {
            return inactiveMembers.isEmpty() ? true
                                            : inactiveMembers.last().equals(self);
        }
        return false;
    }

    /**
     * Set the joining members of the receiver
     * 
     * @param joiningMembers
     */
    public void setJoiningMembers(Node[] joiningMembers) {
        assert joiningMembers != null : "joining members must not be null";
        this.joiningMembers = joiningMembers;
        for (Node node : this.joiningMembers) {
            nextMembership.add(node);
        }
    }

    @Override
    public void stabilized() {
        filterSystemMembership();
        fsm.stabilize();
    }

    protected void advertiseChannelBuffer(Node sender, boolean isActive,
                                          ContactInformation contactInfo) {
        if (isActive) {
            activeWeavers.add(sender);
        } else {
            inactiveWeavers.add(sender);
        }
        yellowPages.put(sender, contactInfo);
    }

    protected void advertiseMember(Node sender, boolean isActive,
                                   ContactInformation contactInfo) {
        if (isActive) {
            activeMembers.add(sender);
        } else {
            inactiveMembers.add(sender);
        }
        if (contactInfo != null) {
            yellowPages.put(sender, contactInfo);
        }
    }

    protected void bootstrapSystem(Node[] joiningMembers) {
        fsm.bootstrapSystem(joiningMembers);
    }

    protected ConsistentHashFunction<Node> calculateNextProducerRing(ConsistentHashFunction<Node> newRing) {
        for (Node node : activeMembers) {
            Node clone = node.clone();
            newRing.add(clone, clone.capacity);
        }
        for (Node node : joiningMembers) {
            Node clone = node.clone();
            newRing.add(clone, clone.capacity);
        }
        return newRing;
    }

    /**
     * 
     */
    protected void cleanUp() {
        joiningMembers = new Node[0];
        joiningWeavers.clear();
        nextMembership.clear();
        tally.set(0);
    }

    /**
     * Coordinate the bootstrapping of the process group.
     */
    abstract protected void coordinateBootstrap();

    /**
     * The receiver is the controller for the group. Coordinate the rebalancing
     * of the system by including the new members.
     */
    protected void coordinateRebalance() {
        if (log.isInfoEnabled()) {
            log.info(String.format("Coordinating rebalancing on %s", self));
        }
        tally.set(nextMembership.size());
        switchboard.ringCast(new Message(
                                         self,
                                         EndpointRebalanceMessage.PREPARE_FOR_REBALANCE,
                                         (Serializable) joiningMembers),
                             nextMembership);
    }

    /**
     * Coordinate the takeover of the completion of the rebalancing
     */
    protected void coordinateTakeover() {
        switchboard.ringCast(new Message(self,
                                         EndpointRebalanceMessage.TAKEOVER),
                             nextMembership);
    }

    /**
     * Failover the process, assuming primary role for any failed primaries this
     * process is serving as the mirror
     */
    abstract protected void failover();

    /**
     * Remove all dead members and partition out the new members from the
     * members that were part of the previous partition
     */
    protected void filterSystemMembership() {
        activeMembers.removeAll(switchboard.getDeadMembers());
        activeWeavers.removeAll(switchboard.getDeadMembers());
        inactiveMembers.removeAll(switchboard.getDeadMembers());
        inactiveWeavers.removeAll(switchboard.getDeadMembers());
        nextMembership.clear();
        nextMembership.addAll(activeMembers);
    }

    protected void initialiateRebalancing(Node[] joiningMembers) {
        fsm.rebalance(joiningMembers);
    }

    /**
     * @param channel
     */
    abstract protected void openChannel(UUID channel);

    abstract protected void rebalance();

    /**
     * Rebalance the responsibilities for this node
     * 
     * @param remapped
     *            - the mapping of the new primary/mirror pairs
     */
    abstract protected void rebalance(Map<UUID, Node[][]> remappping);

    /**
     * Calculate the rebalancing of the system using the supplied list of
     * joining producer processes.
     * 
     * @param joiningMembers
     *            - the list of producers that are joining the process group
     */
    abstract protected void rebalance(Node[] joiningMembers);

    abstract protected void rebalanceComplete();

    protected void rebalanced() {
        activeMembers.clear();
        activeMembers.addAll(nextMembership);
        inactiveMembers.removeAll(nextMembership);
        joiningMembers = new Node[0];
        nextMembership.clear();
        active = true;
        fsm.commitTakeover();
    }

    protected void rebalancePrepared() {
        switchboard.ringCast(new Message(
                                         self,
                                         EndpointRebalanceMessage.INITIATE_REBALANCE),
                             nextMembership);
    }

    /**
     * @return true if the tally is equal to the required size
     */
    protected boolean tallyComplete() {
        return tally.get() == 0;
    }
}