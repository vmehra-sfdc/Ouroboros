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
package com.salesforce.ouroboros.spindle;

import static com.salesforce.ouroboros.util.Utils.point;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.spindle.CoordinatorContext.BootstrapFSM;
import com.salesforce.ouroboros.spindle.CoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.spindle.CoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.spindle.CoordinatorContext.ReplicatorFSM;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestCoordinator {

    private ContactInformation dummyInfo = new ContactInformation(
                                                                  new InetSocketAddress(
                                                                                        0),
                                                                  new InetSocketAddress(
                                                                                        0),
                                                                  new InetSocketAddress(
                                                                                        0));

    @Test
    public void testClose() throws Exception {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(
                                                  timer,
                                                  switchboard,
                                                  weaver,
                                                  new CoordinatorConfiguration());
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
        ContactInformation contactInformation = new ContactInformation(address,
                                                                       address,
                                                                       address);
        ContactInformation contactInformation1 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation2 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation3 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        Map<Node, ContactInformation> newMembers = new HashMap<Node, ContactInformation>();
        newMembers.put(localNode, contactInformation);
        newMembers.put(node1, contactInformation1);
        newMembers.put(node2, contactInformation2);
        newMembers.put(node3, contactInformation3);
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        ring.add(localNode, 1);
        ring.add(node1, 1);
        ring.add(node2, 1);
        ring.add(node3, 1);
        coordinator.setNextRing(ring);
        coordinator.commitNextRing();
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        when(weaver.getReplicationPair(primary)).thenReturn(getPair(primary,
                                                                    ring));
        when(weaver.getReplicationPair(mirror)).thenReturn(getPair(mirror, ring));
        coordinator.open(primary);
        coordinator.open(mirror);
        coordinator.close(primary);
        coordinator.close(mirror);
        verify(weaver).close(primary);
        verify(weaver).close(mirror);
    }

    @Test
    public void testFailover() throws Exception {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(
                                                  timer,
                                                  switchboard,
                                                  weaver,
                                                  new CoordinatorConfiguration());
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
        ContactInformation contactInformation = new ContactInformation(address,
                                                                       address,
                                                                       address);
        ContactInformation contactInformation1 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation2 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation3 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        Map<Node, ContactInformation> newMembers = new HashMap<Node, ContactInformation>();
        newMembers.put(localNode, contactInformation);
        newMembers.put(node1, contactInformation1);
        newMembers.put(node2, contactInformation2);
        newMembers.put(node3, contactInformation3);
        Set<Node> deadMembers = newMembers.keySet();
        when(switchboard.getDeadMembers()).thenReturn(deadMembers);
        coordinator.getFsm().setState(CoordinatorFSM.Failover);
        coordinator.failover();
        assertEquals(CoordinatorFSM.Stable, coordinator.getState());
        verify(weaver).failover(deadMembers);
    }

    @Test
    public void testInitiateBootstrap() throws Exception {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        final Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Switchboard switchboard = mock(Switchboard.class);
        Coordinator coordinator = new Coordinator(
                                                  timer,
                                                  switchboard,
                                                  weaver,
                                                  new CoordinatorConfiguration());
        Answer<Void> answer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(BootstrapMessage.BOOTSTRAP_SPINDLES, message.type);
                Node[] joiningMembers = (Node[]) message.arguments[0];
                assertNotNull(joiningMembers);
                assertEquals(1, joiningMembers.length);
                assertEquals(localNode, joiningMembers[0]);
                return null;
            }
        };
        doAnswer(answer).when(switchboard).ringCast(isA(Message.class));
        coordinator.stabilized();
        assertFalse(coordinator.isActive());
        coordinator.getInactiveMembers().add(localNode);
        coordinator.initiateBootstrap(new Node[] { localNode });
        assertEquals(BootstrapFSM.CoordinateBootstrap, coordinator.getState());
        coordinator.dispatch(BootstrapMessage.BOOTSTRAP_SPINDLES, localNode,
                             new Serializable[] { new Node[] { localNode } }, 0);
        assertEquals(ControllerFSM.CoordinateReplicators,
                     coordinator.getState());
        verify(switchboard, new Times(1)).ringCast(isA(Message.class));
        assertTrue(coordinator.isActive());
    }

    @Test
    public void testOpen() throws Exception {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(
                                                  timer,
                                                  switchboard,
                                                  weaver,
                                                  new CoordinatorConfiguration());
        coordinator.setActive(true);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 2, 2);

        coordinator.getActiveMembers().add(localNode);
        coordinator.getActiveMembers().add(node1);

        when(weaver.getId()).thenReturn(localNode);

        UUID primary = UUID.randomUUID();
        UUID mirror = UUID.randomUUID();

        when(weaver.getReplicationPair(primary)).thenReturn(new Node[] {
                                                                    localNode,
                                                                    node2 });
        when(weaver.getReplicationPair(mirror)).thenReturn(new Node[] { node1,
                                                                   localNode });
        coordinator.open(primary);
        coordinator.open(mirror);
        verify(weaver).openPrimary(eq(primary), (Node) eq(null));
        verify(weaver).openMirror(eq(mirror));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenReplicators() throws Exception {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        when(weaver.getId()).thenReturn(new Node(0, 0, 0));
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(
                                                  timer,
                                                  switchboard,
                                                  weaver,
                                                  new CoordinatorConfiguration());
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
        ContactInformation contactInformation1 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation2 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        ContactInformation contactInformation3 = new ContactInformation(
                                                                        address,
                                                                        address,
                                                                        address);
        coordinator.dispatch(DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER, node1,
                             new Serializable[] { contactInformation1, true },
                             0);
        coordinator.dispatch(DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER, node2,
                             new Serializable[] { contactInformation2, true },
                             0);
        coordinator.dispatch(DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER, node3,
                             new Serializable[] { contactInformation3, true },
                             0);
        coordinator.getInactiveMembers().addAll(Arrays.asList(node1, node2,
                                                              node3));
        coordinator.setJoiningMembers(coordinator.getActiveMembers().toArray(new Node[0]));
        coordinator.getFsm().setState(ReplicatorFSM.EstablishReplicators);
        coordinator.establishReplicators();
        Rendezvous rendezvous = coordinator.getRendezvous();
        assertNotNull(rendezvous);
        assertEquals(3, rendezvous.getParties());

        verify(weaver).openReplicator(node1, contactInformation1, rendezvous);
        verify(weaver).openReplicator(node2, contactInformation2, rendezvous);
        verify(weaver).openReplicator(node3, contactInformation3, rendezvous);
        verify(weaver).connectReplicators(isA(Collection.class), isA(Map.class));
    }

    private Node[] getPair(UUID channel, ConsistentHashFunction<Node> ring) {
        List<Node> pair = ring.hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }
}
