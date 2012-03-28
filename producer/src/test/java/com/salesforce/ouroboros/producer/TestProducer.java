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
package com.salesforce.ouroboros.producer;

import static com.salesforce.ouroboros.util.Utils.point;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.DefaultSkipStrategy;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * @author hhildebrand
 * 
 */
public class TestProducer {

    InetSocketAddress fake = new InetSocketAddress("127.0.0.1", 55433);
    ServerSocket      socket;

    @Before
    public void setup() throws IOException {
        socket = new ServerSocket(fake.getPort(), 100, fake.getAddress());
    }

    @After
    public void shutdown() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }

    @Test
    public void testFailover() throws IOException, InterruptedException {
        EventSource source = mock(EventSource.class);
        ProducerConfiguration configuration = new ProducerConfiguration();
        ConsistentHashFunction<Node> producerRing = new ConsistentHashFunction<Node>(
                                                                                     new DefaultSkipStrategy());
        Node[] producerNodes = new Node[3];
        producerNodes[0] = new Node(0);
        producerNodes[1] = new Node(1);
        producerNodes[2] = new Node(2);
        for (Node n : producerNodes) {
            producerRing.add(n, 1);
        }
        UUID channel = UUID.randomUUID();
        List<Node> mapping = producerRing.hash(point(channel), 2);

        Node self = mapping.get(1); // we are the secondary

        ContactInformation info = new ContactInformation(fake, fake, fake);
        Map<Node, ContactInformation> yellowPages = new HashMap<Node, ContactInformation>();
        ConsistentHashFunction<Node> spindleRing = new ConsistentHashFunction<Node>(
                                                                                    new DefaultSkipStrategy());
        SortedSet<Node> spindleNodes = new TreeSet<Node>();
        spindleNodes.add(new Node(3));
        spindleNodes.add(new Node(4));
        spindleNodes.add(new Node(5));
        for (Node n : spindleNodes) {
            spindleRing.add(n, 1);
            yellowPages.put(n, info);
        }

        Producer producer = new Producer(self, source, configuration);
        producer.setProducerRing(producerRing);
        producer.createSpinners(spindleNodes, yellowPages);
        producer.remapWeavers(spindleRing);

        producer.opened(channel);

        Thread.sleep(15000);
    }
}
