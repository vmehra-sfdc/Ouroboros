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
public class ReplicatorStateMachine implements StateMachine {

    public enum State {
        ERROR, REBALANCED, STABLIZED, SYNCHRONIZED, SYNCHRONIZING, UNSTABLE,
        UNSYNCHRONIZED;
    }

    protected static final Logger             log                  = Logger.getLogger(ReplicatorStateMachine.class.getCanonicalName());

    private final AtomicReference<Rendezvous> replicatorRendezvous = new AtomicReference<Rendezvous>();
    protected final Coordinator               coordinator;
    protected final AtomicReference<State>    state                = new AtomicReference<State>(
                                                                                                State.UNSTABLE);

    public ReplicatorStateMachine(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void destabilize() {
        State prev = state.getAndSet(State.UNSTABLE);
        switch (prev) {
            default:
        }
    }

    public State getState() {
        return state.get();
    }

    public void partitionSynchronized(Node leader) {
        if (!state.compareAndSet(State.SYNCHRONIZING, State.SYNCHRONIZED)) {
            state.set(State.ERROR);
            throw new IllegalStateException(
                                            String.format("Can only transition to SYNCHRONIZED from the SYNCHRONIZING state: %s",
                                                          state.get()));
        }
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Partition replicator synchronization achieved on %s",
                                   coordinator.getId()));
        }
        replicatorRendezvous.getAndSet(null).cancel();
    }

    public void replicatorsSynchronizedOn(Node sender) {
        state.set(State.ERROR);
        throw new IllegalStateException(
                                        "Transition handled by coordinator only");
    }

    public void replicatorSynchronizeFailed(Node sender) {
        state.set(State.ERROR);
        throw new IllegalStateException(
                                        "Transition handled by coordinator only");
    }

    @Override
    public void stabilized() {
        if (!state.compareAndSet(State.UNSTABLE, State.STABLIZED)) {
            state.set(State.ERROR);
            throw new IllegalStateException(
                                            String.format("Can only stabilize in the unstable state: %s",
                                                          state.get()));
        }
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
        if (!state.compareAndSet(State.STABLIZED, State.SYNCHRONIZING)) {
            state.set(State.ERROR);
            throw new IllegalStateException(
                                            String.format("Can only transition to synchronizing in state STABILIZED: ",
                                                          state.get()));
        }
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Starting replicator synchronization on %s",
                                   coordinator.getId()));
        }
        Runnable action = new Runnable() {
            @Override
            public void run() {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Replicators synchronization achieved on %s",
                                           coordinator.getId()));
                }
                coordinator.getSwitchboard().send(new Message(
                                                              coordinator.getId(),
                                                              ReplicatorSynchronization.REPLICATORS_SYNCHRONIZED),
                                                  leader);
            }
        };
        Runnable timeoutAction = new Runnable() {
            @Override
            public void run() {
                try {
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Replicators synchronization timed out on %s",
                                               coordinator.getId()));
                    }
                    coordinator.getSwitchboard().send(new Message(
                                                                  coordinator.getId(),
                                                                  ReplicatorSynchronization.REPLICATOR_SYNCHRONIZATION_FAILED),
                                                      leader);
                } catch (Throwable e) {
                    state.set(State.ERROR);
                    log.log(Level.SEVERE,
                            "Timeout action for replicator synchronization rendezvous failed",
                            e);
                }
            }
        };
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Synchronization of replicators initiated on %s",
                                   coordinator.getId()));
        }
        replicatorRendezvous.set(coordinator.openReplicators(coordinator.getNewMembers(),
                                                             action,
                                                             timeoutAction));
    }

    /**
     * The synchronization of the replicators in the group failed.
     * 
     * @param leader
     *            - the leader of the spindle group
     */
    public void synchronizeReplicatorsFailed(Node leader) {
        if (replicatorRendezvous.get().cancel()) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Partition Replicator synchronization failed on %s",
                                       coordinator.getId()));
            }
            state.compareAndSet(State.SYNCHRONIZING, State.UNSYNCHRONIZED);
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.orchestration.StateMachine#transition(com.salesforce.ouroboros.spindle.orchestration.ReplicatorSynchronization, com.salesforce.ouroboros.Node, java.io.Serializable, long)
     */
    @Override
    public void transition(ReplicatorSynchronization type, Node sender,
                           Serializable payload, long time) {
        type.dispatch(this, sender, payload, time);
    }

    @Override
    public void transition(StateMachineDispatch type, Node sender,
                           Serializable payload, long time) {
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("transitioning %s node %s payload %s @ %s",
                                    type, sender, payload, time));
        }
        type.dispatch(this, sender, payload, time);
    }
}
