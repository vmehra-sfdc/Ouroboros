package com.salesforce.ouroboros.spindle.orchestration;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.spindle.orchestration.Coordinator.State;
import com.salesforce.ouroboros.util.Rendezvous;

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

    @Override
    public void transition(ReplicatorSynchronization type, Node sender,
                           Serializable payload, long time) {
        type.dispatch(this, sender, payload, time);
    }
}
