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

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Test;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.orchestration.ReplicatorStateMachine.State;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestReplicatorStateMachine {

    @Test
    public void testStabilized() {
        Coordinator coordinator = mock(Coordinator.class);
        ReplicatorStateMachine sm = new ReplicatorStateMachine(coordinator);
        assertEquals(State.UNSTABLE, sm.getState());
        sm.stabilized();
        assertEquals(State.STABLIZED, sm.getState());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSynchronize() {
        Node node = new Node(0, 0, 0);
        Coordinator coordinator = mock(Coordinator.class);
        ReplicatorStateMachine sm = new ReplicatorStateMachine(coordinator);
        assertEquals(State.UNSTABLE, sm.getState());
        sm.stabilized();
        assertEquals(State.STABLIZED, sm.getState());
        sm.transition(ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS, node,
                      null, 1);
        assertEquals(State.SYNCHRONIZING, sm.getState());
        verify(coordinator).openReplicators(isA(Collection.class),
                                            isA(Runnable.class),
                                            isA(Runnable.class));
    }

    @Test
    public void testSynchronized() {
        Node node = new Node(0, 0, 0);
        Coordinator coordinator = mock(Coordinator.class);
        ReplicatorStateMachine sm = new ReplicatorStateMachine(coordinator);
        assertEquals(State.UNSTABLE, sm.getState());
        sm.stabilized();
        assertEquals(State.STABLIZED, sm.getState());
        sm.transition(ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS, node,
                      null, 1);
        assertEquals(State.SYNCHRONIZING, sm.getState());
        sm.transition(ReplicatorSynchronization.PARTITION_SYNCHRONIZED, node,
                      null, 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSynchronizeFailed() {
        Node node = new Node(0, 0, 0);
        Coordinator coordinator = mock(Coordinator.class);
        Rendezvous rendezvous = mock(Rendezvous.class);
        when(
             coordinator.openReplicators(isA(Collection.class),
                                         isA(Runnable.class),
                                         isA(Runnable.class))).thenReturn(rendezvous);
        ReplicatorStateMachine sm = new ReplicatorStateMachine(coordinator);
        assertEquals(State.UNSTABLE, sm.getState());
        sm.stabilized();
        assertEquals(State.STABLIZED, sm.getState());
        sm.transition(ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS, node,
                      null, 1);
        assertEquals(State.SYNCHRONIZING, sm.getState());
        sm.transition(ReplicatorSynchronization.SYNCHRONIZE_REPLICATORS_FAILED,
                      node, null, 0);
        verify(rendezvous).cancel();
    }
}
