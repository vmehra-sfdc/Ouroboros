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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.mockito.internal.verification.Times;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

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
    public void testClose() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
        Node requester = new Node(-1, -1, -1);
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
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary, requester);
        coordinator.open(mirror, requester);
        coordinator.close(primary, requester);
        coordinator.close(mirror, requester);
        verify(weaver).close(primary);
        verify(weaver).close(mirror);
        verify(switchboard, new Times(4)).send(isA(Message.class),
                                               eq(requester));
    }

    @Test
    public void testFailover() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
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
        coordinator.failover();

        verify(weaver).failover(deadMembers);
        verify(weaver).closeReplicator(localNode);
        verify(weaver).closeReplicator(node1);
        verify(weaver).closeReplicator(node2);
        verify(weaver).closeReplicator(node2);
    }

    @Test
    public void testOpen() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
        Node requester = new Node(-1, -1, -1);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        ring.add(localNode, 1);
        ring.add(node1, 1);
        ring.add(node2, 1);
        ring.add(node3, 1);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary, requester);
        coordinator.openMirror(mirror, requester);
        verify(weaver).openPrimary(eq(primary), isA(Node.class));
        verify(weaver).openMirror(eq(mirror), isA(Node.class));
    }

    @Test
    public void testOpenReplicators() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        when(weaver.getId()).thenReturn(new Node(0, 0, 0));
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
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
        coordinator.dispatch(GlobalMessageType.ADVERTISE_CHANNEL_BUFFER, node1,
                             contactInformation1, 0);
        coordinator.dispatch(GlobalMessageType.ADVERTISE_CHANNEL_BUFFER, node2,
                             contactInformation2, 0);
        coordinator.dispatch(GlobalMessageType.ADVERTISE_CHANNEL_BUFFER, node3,
                             contactInformation3, 0);
        coordinator.newMembers.addAll(Arrays.asList(node1, node2, node3));
        Rendezvous rendezvous = coordinator.openReplicators();
        assertNotNull(rendezvous);
        assertEquals(3, rendezvous.getParties());

        verify(weaver).openReplicator(node1, contactInformation1, rendezvous);
        verify(weaver).openReplicator(node2, contactInformation2, rendezvous);
        verify(weaver).openReplicator(node3, contactInformation3, rendezvous);
    }

    @Test
    public void testRebalance() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Switchboard switchboard = mock(Switchboard.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
        Node requester = new Node(-1, -1, -1);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        ring.add(localNode, 1);
        ring.add(node1, 1);
        ring.add(node2, 1);
        ring.add(node3, 1);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary, requester);
        coordinator.open(mirror, requester);
        Node removedMirror = coordinator.getReplicationPair(primary)[1];
        Node removedPrimary = coordinator.getReplicationPair(mirror)[0];

        ConsistentHashFunction<Node> newRing = ring.clone();
        newRing.remove(removedMirror);
        newRing.remove(removedPrimary);
        Map<UUID, Node[]> remapped = coordinator.remap(newRing);
        List<Node> deadMembers = Arrays.asList(removedPrimary, removedMirror);
        coordinator.rebalance(remapped, deadMembers);
        verify(weaver).rebalance(primary, remapped.get(primary), deadMembers);
        verify(weaver).rebalance(mirror, remapped.get(mirror), deadMembers);
    }

    @Test
    public void testRemap() {
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        Weaver weaver = mock(Weaver.class);
        Node localNode = new Node(0, 0, 0);
        when(weaver.getId()).thenReturn(localNode);
        when(weaver.getContactInformation()).thenReturn(dummyInfo);
        Switchboard switchboard = mock(Switchboard.class);
        Coordinator coordinator = new Coordinator(timer, switchboard, weaver);
        Node requester = new Node(-1, -1, -1);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        ring.add(localNode, 1);
        ring.add(node1, 1);
        ring.add(node2, 1);
        ring.add(node3, 1);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary, requester);
        coordinator.open(mirror, requester);
        ConsistentHashFunction<Node> newRing = ring.clone();
        newRing.remove(coordinator.getReplicationPair(primary)[1]);
        newRing.remove(coordinator.getReplicationPair(mirror)[0]);
        Map<UUID, Node[]> remapped = coordinator.remap(newRing);
        assertNotNull(remapped);
        assertEquals(2, remapped.size());
        assertNotNull(remapped.get(primary));
        assertNotNull(remapped.get(mirror));
    }
}
