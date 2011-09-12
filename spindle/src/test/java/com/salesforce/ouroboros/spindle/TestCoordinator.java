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
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestCoordinator {
    @Test
    public void testOpen() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        ConsistentHashFunction<Node> ring = coordinator.addNewMembers(newMembers);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary);
        coordinator.open(mirror);
        verify(weaver).openPrimary(eq(primary), isA(Node.class));
        verify(weaver).openMirror(eq(mirror), isA(Node.class));
    }

    @Test
    public void testClose() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        ConsistentHashFunction<Node> ring = coordinator.addNewMembers(newMembers);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary);
        coordinator.open(mirror);
        coordinator.close(primary);
        coordinator.close(mirror);
        verify(weaver).close(primary);
        verify(weaver).close(mirror);
    }

    @Test
    public void testAddNewMembers() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        ConsistentHashFunction<Node> remapped = coordinator.addNewMembers(newMembers);
        assertNotNull(remapped);
        assertEquals(4, remapped.size());
        assertTrue(remapped.getBuckets().contains(localNode));
        assertTrue(remapped.getBuckets().contains(node1));
        assertTrue(remapped.getBuckets().contains(node2));
        assertTrue(remapped.getBuckets().contains(node3));

        assertEquals(contactInformation,
                     coordinator.getContactInformationFor(localNode));
        assertEquals(contactInformation1,
                     coordinator.getContactInformationFor(node1));
        assertEquals(contactInformation2,
                     coordinator.getContactInformationFor(node2));
        assertEquals(contactInformation3,
                     coordinator.getContactInformationFor(node3));
    }

    @Test
    public void testFailover() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        coordinator.addNewMembers(newMembers);
        Set<Node> deadMembers = newMembers.keySet();
        coordinator.failover(deadMembers);

        assertNull(coordinator.getContactInformationFor(localNode));
        assertNull(coordinator.getContactInformationFor(node1));
        assertNull(coordinator.getContactInformationFor(node2));
        assertNull(coordinator.getContactInformationFor(node3));

        verify(weaver).failover(deadMembers);
        verify(weaver).closeReplicator(localNode);
        verify(weaver).closeReplicator(node1);
        verify(weaver).closeReplicator(node2);
        verify(weaver).closeReplicator(node2);
    }

    @Test
    public void testRemap() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        ConsistentHashFunction<Node> ring = coordinator.addNewMembers(newMembers);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary);
        coordinator.open(mirror);
        ConsistentHashFunction<Node> newRing = ring.clone();
        newRing.remove(coordinator.getReplicationPair(primary)[1]);
        newRing.remove(coordinator.getReplicationPair(mirror)[0]);
        Map<UUID, Node[]> remapped = coordinator.remap(newRing);
        assertNotNull(remapped);
        assertEquals(2, remapped.size());
        assertNotNull(remapped.get(primary));
        assertNotNull(remapped.get(mirror));
    }

    @Test
    public void testRebalance() {
        Weaver weaver = mock(Weaver.class);
        Coordinator coordinator = new Coordinator();
        coordinator.ready(weaver);
        Node localNode = new Node(0, 0, 0);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);

        when(weaver.getId()).thenReturn(localNode);

        InetSocketAddress address = new InetSocketAddress(0);
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
        ConsistentHashFunction<Node> ring = coordinator.addNewMembers(newMembers);
        coordinator.updateRing(ring);
        UUID primary = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
            }
        }
        UUID mirror = null;
        while (mirror == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Coordinator.point(test), 2);
            if (pair.get(1).equals(localNode)) {
                mirror = test;
            }
        }
        coordinator.open(primary);
        coordinator.open(mirror);
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
}
