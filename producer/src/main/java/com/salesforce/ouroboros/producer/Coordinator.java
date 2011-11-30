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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import statemap.StateUndefinedException;

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
    private Node[]                              joiningWeavers  = new Node[0];
    private ConsistentHashFunction<Node>        nextWeaverRing  = new ConsistentHashFunction<Node>();
    private final Producer                      producer;
    private final Node                          self;
    private final Switchboard                   switchboard;
    private final Map<Node, ContactInformation> yellowPages     = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(Switchboard switchboard, Producer producer)
                                                                  throws IOException {
        this.producer = producer;
        self = producer.getId();
        switchboard.setMember(this);
        this.switchboard = switchboard;
    }

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(self,
                                         GlobalMessageType.ADVERTISE_PRODUCER,
                                         active));
    }

    @Override
    public void destabilize() {
        fsm.destabilized();
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case OPENED: {
                break;
            }
            case CLOSED: {
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
    public void dispatch(FailoverMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case PREPARE:
                // do nothing
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
    public void dispatch(GlobalMessageType type, Node sender,
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
    public void dispatch(MemberDispatch type, Node sender,
                         Serializable[] arguments, long time) {
    }

    @Override
    public void dispatch(RebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case BOOTSTRAP:
                joiningWeavers = (Node[]) arguments[0];
                switchboard.ringCast(new Message(sender, type, arguments));
                break;
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

    @Override
    public void stabilized() {
        fsm.stabilized();
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
}
