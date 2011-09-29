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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;

import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.orchestration.Coordinator;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestWeaver {
    @Test
    public void testClose() throws Exception {
        UUID channel = UUID.randomUUID();
        Node id = new Node(0, 0, 0);
        Node mirror = new Node(1, 0, 0);
        EventHeader header = mock(EventHeader.class);
        when(header.getChannel()).thenReturn(channel);
        File root = File.createTempFile("weaver", ".root");
        root.delete();
        assertTrue(root.mkdirs());
        root.deleteOnExit();
        Coordinator coordinator = mock(Coordinator.class);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.setRoot(root);
        Weaver weaver = new Weaver(config, coordinator);
        weaver.openPrimary(channel, mirror);
        assertNotNull(weaver.eventChannelFor(header));
        weaver.close(channel);
        assertNull(weaver.eventChannelFor(header));
    }

    @Test
    public void testOpenMirror() throws Exception {
        UUID channel = UUID.randomUUID();
        UUID channel2 = UUID.randomUUID();
        assertFalse(channel.equals(channel2));
        Node id = new Node(0, 0, 0);
        Node primary = new Node(1, 0, 0);
        EventHeader header = mock(EventHeader.class);
        when(header.getChannel()).thenReturn(channel);
        EventHeader header2 = mock(EventHeader.class);
        when(header2.getChannel()).thenReturn(channel2);
        File root = File.createTempFile("weaver", ".root");
        root.delete();
        assertTrue(root.mkdirs());
        root.deleteOnExit();
        Coordinator coordinator = mock(Coordinator.class);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.setRoot(root);
        Weaver weaver = new Weaver(config, coordinator);
        weaver.openMirror(channel, primary);
        EventChannel eventChannel = weaver.eventChannelFor(header);
        assertNotNull(eventChannel);
        assertTrue(eventChannel.isMirror());
        assertNull(weaver.eventChannelFor(header2));
    }

    @Test
    public void testOpenPrimary() throws Exception {
        UUID channel = UUID.randomUUID();
        UUID channel2 = UUID.randomUUID();
        assertFalse(channel.equals(channel2));
        Node id = new Node(0, 0, 0);
        Node mirror = new Node(1, 0, 0);
        EventHeader header = mock(EventHeader.class);
        when(header.getChannel()).thenReturn(channel);
        EventHeader header2 = mock(EventHeader.class);
        when(header2.getChannel()).thenReturn(channel2);
        File root = File.createTempFile("weaver", ".root");
        root.delete();
        assertTrue(root.mkdirs());
        root.deleteOnExit();
        Coordinator coordinator = mock(Coordinator.class);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.setRoot(root);
        Weaver weaver = new Weaver(config, coordinator);
        weaver.openPrimary(channel, mirror);
        EventChannel eventChannel = weaver.eventChannelFor(header);
        assertNotNull(eventChannel);
        assertTrue(eventChannel.isPrimary());
        assertNull(weaver.eventChannelFor(header2));
    }

    @Test
    public void testOpenReplicator() throws Exception {
        Rendezvous rendezvous = mock(Rendezvous.class);
        SocketOptions options = new SocketOptions();
        options.setTimeout(100);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        options.configure(server.socket());
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
        File root = File.createTempFile("weaver", ".root");
        root.delete();
        assertTrue(root.mkdirs());
        root.deleteOnExit();
        Coordinator coordinator = mock(Coordinator.class);
        WeaverConfigation config = new WeaverConfigation();
        config.setId(id);
        config.setRoot(root);
        Weaver weaver = new Weaver(config, coordinator);
        weaver.start();
        weaver.openReplicator(mirror, info, rendezvous);
        SocketChannel connected = server.accept();
        assertNotNull(connected);
        assertTrue(connected.isConnected());

        ByteBuffer buffer = ByteBuffer.allocate(Replicator.HANDSHAKE_SIZE);
        connected.read(buffer);
        buffer.flip();
        assertEquals(Replicator.MAGIC, buffer.getInt());
        Node handshakeNode = new Node(buffer);
        assertEquals(id, handshakeNode);
        Thread.sleep(10); // Time for replicator to await at the barrier
        verify(rendezvous).meet();
        weaver.terminate();
        connected.close();
        server.close();
    }
}
