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

import java.util.SortedSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ReplicationCoordinatorStateMachine extends ReplicatorStateMachine {
    private final static Logger               log                  = Logger.getLogger(ReplicationCoordinatorStateMachine.class.getCanonicalName());

    private final AtomicReference<Rendezvous> coordinatorRendevous = new AtomicReference<Rendezvous>();

    public ReplicationCoordinatorStateMachine(Coordinator coordinator) {
        super(coordinator);
    }

    /**
     * The replicators on the node have been synchronized
     * 
     * @param sender
     *            - the node where the replicators have been synchronized
     */
    @Override
    public void replicatorsSynchronizedOn(Node sender) {
        try {
            coordinatorRendevous.get().meet();
        } catch (BrokenBarrierException e) {
            if (log.isLoggable(Level.FINE)) {
                log.log(Level.FINE,
                        String.format("Replicator coordination on leader %s has failed; update from %s",
                                      coordinator.getId(), sender), e);
            }
        }
    }

    /**
     * The replicators on the node have failed to synchronize. Drop back ten and
     * punt
     * 
     * @param sender
     *            - the node where the replicators failed to synchronize
     */
    @Override
    public void replicatorSynchronizeFailed(Node sender) {
        if (coordinatorRendevous.get().cancel()) {
            if (state.compareAndSet(State.SYNCHRONIZING, State.UNSYNCHRONIZED)) {
                coordinator.getSwitchboard().broadcast(new Message(
                                                                   coordinator.getId(),
                                                                   ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS_FAILED),
                                                       coordinator.getMembers());
            }
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.orchestration.ReplicatorStateMachine#stabilized()
     */
    @Override
    public void stabilized() {
        super.stabilized();
        // Schedule the coordination rendezvous to synchronize the group members
        Runnable action = new Runnable() {
            @Override
            public void run() {
            }

        };
        Runnable timeoutAction = new Runnable() {

            @Override
            public void run() {

            }

        };
        SortedSet<Node> members = coordinator.getMembers();
        Rendezvous rendezvous = new Rendezvous(members.size(), action);
        rendezvous.scheduleCancellation(Coordinator.DEFAULT_TIMEOUT,
                                        Coordinator.TIMEOUT_UNIT,
                                        coordinator.getTimer(), timeoutAction);
        coordinatorRendevous.set(rendezvous);
        // Start the replicator synchronization
        coordinator.getSwitchboard().broadcast(new Message(
                                                           coordinator.getId(),
                                                           ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS),
                                               members);
    }
}
