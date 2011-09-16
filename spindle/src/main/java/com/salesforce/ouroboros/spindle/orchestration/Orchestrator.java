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

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.views.View;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.Switchboard;

/**
 * This class is a state machine driven by the partitioning events within the
 * group membership as well as by messages sent within the group. This class
 * orchestrates the distributed {@link Coordinator} for the channel buffer
 * process.
 * 
 * @author hhildebrand
 * 
 */
public class Orchestrator extends Switchboard {

    final static Logger                                   log         = Logger.getLogger(Orchestrator.class.getCanonicalName());

    private final List<Node>                              deadMembers = new CopyOnWriteArrayList<Node>();
    private final ConcurrentMap<Node, ContactInformation> members     = new ConcurrentHashMap<Node, ContactInformation>();
    private final ConcurrentMap<Node, ContactInformation> newMembers  = new ConcurrentHashMap<Node, ContactInformation>();
    private final AtomicReference<State>                  state       = new AtomicReference<State>(
                                                                                                   State.INITIAL);

    public Orchestrator(Node node, Partition p) {
        super(node, p);
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
                                   self, view, leader));
        }
        deadMembers.clear();
        newMembers.clear();
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
        if (members.containsKey(self)) {
        } else {
            newMembers.putIfAbsent(self, card);
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
        for (Entry<Node, ContactInformation> entry : members.entrySet()) {
            Node member = entry.getKey();
            if (!view.contains(member.processId)) {
                deadMembers.add(member);
                members.remove(member);
            }
        }
    }

    /**
     * The state machine driver
     * 
     * @param next
     *            - the next state
     */
    void transitionTo(State next) {
        state.getAndSet(next).next(next, this);
    }

    @Override
    public void discoverChannelBuffer(Node sender, ContactInformation info,
                                      long time) {
    }
}
