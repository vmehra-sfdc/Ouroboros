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

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum ReplicatorSynchronization implements MemberDispatch {
    REPLICATOR_SYNCHRONIZATION_FAILED() {
        @Override
        public void dispatch(ReplicationCoordinatorStateMachine sm,
                             Node sender, Serializable payload, long time) {
            sm.replicatorSynchronizeFailed(sender);
        }

        @Override
        void dispatch(ReplicatorStateMachine sm, Node sender,
                      Serializable payload, long time) {
            throw new UnsupportedOperationException();
        }
    },
    REPLICATORS_SYNCHRONIZED() {
        @Override
        public void dispatch(ReplicationCoordinatorStateMachine sm,
                             Node sender, Serializable payload, long time) {
            sm.replicatorsSynchronizedOn(sender);
        }

        @Override
        void dispatch(ReplicatorStateMachine sm, Node sender,
                      Serializable payload, long time) {
            throw new UnsupportedOperationException();
        }
    },
    SYNCHRONIZE_REPLICATORS() {
        @Override
        public void dispatch(ReplicatorStateMachine sm, Node sender,
                             Serializable payload, long time) {
            sm.synchronizeReplicators(sender);
        }
    },
    SYNCHRONIZE_REPLICATORS_FAILED() {
        @Override
        public void dispatch(ReplicatorStateMachine sm, Node leader,
                             Serializable payload, long time) {
            sm.synchronizeReplicatorsFailed(leader);
        }
    };

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.MemberDispatch#dispatch(com.salesforce.ouroboros.partition.Switchboard.Member, com.salesforce.ouroboros.Node, java.io.Serializable, long)
     */
    @Override
    public void dispatch(Member member, Node sender, Serializable payload,
                         long time) {
        ((Coordinator) member).transition(this, sender, payload, time);
    }

    @Override
    public void dispatch(Switchboard switchboard, Node sender,
                         Serializable payload, long time) {
        switchboard.dispatchToMember(this, sender, payload, time);
    }

    void dispatch(ReplicationCoordinatorStateMachine sm, Node sender,
                  Serializable payload, long time) {
        dispatch((ReplicatorStateMachine) sm, sender, payload, time);
    }

    abstract void dispatch(ReplicatorStateMachine sm, Node sender,
                           Serializable payload, long time);
}