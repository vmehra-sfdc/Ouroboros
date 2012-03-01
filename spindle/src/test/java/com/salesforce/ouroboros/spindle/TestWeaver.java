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

import static com.salesforce.ouroboros.util.Utils.deleteDirectory;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.testUtils.Util.Condition;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestWeaver {

    File root;

    @Before
    public void buildRoot() throws Exception {
        root = File.createTempFile("weaver", ".root");
        root.delete();
        assertTrue(root.mkdirs());
        root.deleteOnExit();
    }

    @After
    public void deleteRoot() throws Exception {
        deleteDirectory(root);
    }

    @Test
    public void testClose() throws Exception {
        UUID channel = UUID.randomUUID();
        Node id = new Node(0, 0, 0);
        Node mirror = new Node(1, 0, 0);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.addRoot(root);
        Weaver weaver = new Weaver(config);
        weaver.openPrimary(channel, mirror);
        assertNotNull(weaver.eventChannelFor(channel));
        weaver.close(channel);
        assertNull(weaver.eventChannelFor(channel));
    }

    @Test
    public void testOpenMirror() throws Exception {
        UUID channel = UUID.randomUUID();
        UUID channel2 = UUID.randomUUID();
        assertFalse(channel.equals(channel2));
        Node primary = new Node(1);
        Node id = new Node(0);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.addRoot(root);
        Weaver weaver = new Weaver(config);
        weaver.openMirror(channel, primary);
        EventChannel eventChannel = weaver.eventChannelFor(channel);
        assertNotNull(eventChannel);
        assertTrue(eventChannel.isMirror());
        assertNull(weaver.eventChannelFor(channel2));
    }

    @Test
    public void testOpenPrimary() throws Exception {
        UUID channel = UUID.randomUUID();
        UUID channel2 = UUID.randomUUID();
        assertFalse(channel.equals(channel2));
        Node id = new Node(0, 0, 0);
        Node mirror = new Node(1, 0, 0);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.addRoot(root);
        Weaver weaver = new Weaver(config);
        weaver.openPrimary(channel, mirror);
        EventChannel eventChannel = weaver.eventChannelFor(channel);
        assertNotNull(eventChannel);
        assertTrue(eventChannel.isPrimary());
        assertNull(weaver.eventChannelFor(channel2));
    }

    @Test
    public void testOpenReplicator() throws Exception {
        final AtomicBoolean met = new AtomicBoolean();
        Rendezvous rendezvous = new Rendezvous(1, new Runnable() {
            @Override
            public void run() {
                met.set(true);
            }

        }, null);
        SocketOptions options = new SocketOptions();
        options.setTimeout(1000);
        ServerSocketChannel server = ServerSocketChannel.open();
        options.configure(server.socket());
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        ContactInformation info = new ContactInformation(
                                                         new InetSocketAddress(
                                                                               "127.0.0.1",
                                                                               0),
                                                         new InetSocketAddress(
                                                                               server.socket().getInetAddress(),
                                                                               server.socket().getLocalPort()),
                                                         new InetSocketAddress(
                                                                               "127.0.0.1",
                                                                               0));
        Node id = new Node(0, 0, 0);
        Node mirror = new Node(1, 0, 0);
        HashMap<Node, ContactInformation> yellowPages = new HashMap<Node, ContactInformation>();
        yellowPages.put(mirror, info);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.addRoot(root);
        Weaver weaver = new Weaver(config);
        weaver.start();
        final Replicator replicator = weaver.openReplicator(mirror, info,
                                                            rendezvous);
        weaver.connectReplicators(Arrays.asList(replicator), yellowPages);
        SocketChannel connected = server.accept();
        assertNotNull(connected);
        assertTrue(connected.isConnected());

        ByteBuffer buffer = ByteBuffer.allocate(Replicator.HANDSHAKE_SIZE);
        connected.read(buffer);
        buffer.flip();
        assertEquals(Replicator.MAGIC, buffer.getInt());
        Node handshakeNode = new Node(buffer);
        assertEquals(id, handshakeNode);
        Util.waitFor("Replicator rendezvous never met", new Condition() {
            @Override
            public boolean value() {
                return met.get();
            }
        }, 10000, 100);

        weaver.terminate();
        connected.close();
        server.close();
    }

    @Test
    public void testRemap() throws Exception {
        Node localNode = new Node(0, 0, 0);
        WeaverConfigation configuration = new WeaverConfigation();
        configuration.setId(localNode);
        configuration.addRoot(root);
        Weaver weaver = new Weaver(configuration);
        Node node1 = new Node(1, 1, 1);
        Node node2 = new Node(2, 1, 1);
        Node node3 = new Node(3, 1, 1);
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        ring.add(localNode, 1);
        ring.add(node1, 1);
        ring.add(node2, 1);
        ring.add(node3, 1);
        weaver.setRing(ring);
        UUID primary = null;
        Node primaryNode = null;
        Node mirrorNode = null;
        while (primary == null) {
            UUID test = UUID.randomUUID();
            List<Node> pair = ring.hash(Utils.point(test), 2);
            if (pair.get(0).equals(localNode)) {
                primary = test;
                mirrorNode = pair.get(1);
                primaryNode = pair.get(0);
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

        weaver.openPrimary(primary, mirrorNode);
        weaver.openMirror(mirror, primaryNode);

        ConsistentHashFunction<Node> newRing = ring.clone();
        newRing.remove(weaver.getReplicationPair(primary)[1]);
        newRing.remove(weaver.getReplicationPair(mirror)[0]);
        weaver.setNextRing(newRing);
        Map<UUID, Node[][]> remapped = weaver.remap();
        assertNotNull(remapped);
        assertEquals(2, remapped.size());
        assertNotNull(remapped.get(primary));
        assertNotNull(remapped.get(mirror));
    }
}
