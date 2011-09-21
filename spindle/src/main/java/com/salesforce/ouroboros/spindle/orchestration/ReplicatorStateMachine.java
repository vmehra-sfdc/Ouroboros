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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.spindle.orchestration.Coordinator.State;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ReplicatorStateMachine extends StateMachine {
    private final AtomicReference<Rendezvous> replicatorRendezvous = new AtomicReference<Rendezvous>();
    protected final Logger                    log                  = Logger.getLogger(ReplicatorStateMachine.class.getCanonicalName());
    protected final AtomicReference<State>    state                = new AtomicReference<State>();

    public ReplicatorStateMachine(Coordinator coordinator) {
        super(coordinator);
    }

    @Override
    public void destabilize() {
    }

    public void replicatorsSynchronizedOn(Node sender) {
    }

    public void replicatorSynchronizeFailed(Node sender) {
        // TODO Auto-generated method stub

    }

    /**
     * Synchronize the replicators on this node. Set up the replicator
     * rendezvous for local replicators, notifying the group leader when the
     * replicators have been synchronized or whether the synchronization has
     * failed.
     * 
     * @param leader
     *            - the group leader
     */
    public void synchronizeReplicators(final Node leader) {
        Runnable action = new Runnable() {
            @Override
            public void run() {
                coordinator.getSwitchboard().send(new Message(
                                                              coordinator.getId(),
                                                              ReplicatorSynchronization.REPLICATORS_SYNCHRONIZED),
                                                  leader);
            }
        };
        Runnable cancelledAction = new Runnable() {
            @Override
            public void run() {
                coordinator.getSwitchboard().send(new Message(
                                                              coordinator.getId(),
                                                              ReplicatorSynchronization.REPLICATOR_SYNCHRONIZATION_FAILED),
                                                  leader);
            }
        };
        replicatorRendezvous.set(coordinator.openReplicators(coordinator.getNewMembers(),
                                                             action,
                                                             cancelledAction));
    }

    /**
     * The synchronization of the replicators in the group failed.
     * 
     * @param leader
     *            - the leader of the spindle group
     */
    public void synchronizeReplicatorsFailed(Node leader) {
        if (replicatorRendezvous.get().cancel()) {
            state.compareAndSet(State.SYNCHRONIZING, State.UNSYNCHRONIZED);
        }
    }

    @Override
    public void transition(ReplicatorSynchronization type, Node sender,
                           Serializable payload, long time) {
        type.dispatch(this, sender, payload, time);
    }
}
