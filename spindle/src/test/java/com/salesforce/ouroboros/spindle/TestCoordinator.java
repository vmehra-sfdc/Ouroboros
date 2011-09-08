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

import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import static junit.framework.Assert.*;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestCoordinator {
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
}
