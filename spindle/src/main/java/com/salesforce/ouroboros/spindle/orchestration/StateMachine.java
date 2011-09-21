package com.salesforce.ouroboros.spindle.orchestration;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;

public abstract class StateMachine {
    protected final Coordinator coordinator;

    public StateMachine(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void transition(ReplicatorSynchronization type, Node sender,
                           Serializable payload, long time) {
        throw new UnsupportedOperationException();
    }

    void destabilize() {

    }

    void stabilized() {

    }
}
