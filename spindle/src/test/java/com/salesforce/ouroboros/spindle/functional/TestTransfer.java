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
package com.salesforce.ouroboros.spindle.functional;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.source.Spindle;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestTransfer {

    private File primaryRoot;
    private File mirrorRoot;

    @Test
    public void testReplicationTransfer() throws Exception {
        Node producerNode = new Node(0);
        Node primaryNode = new Node(1);
        Node mirrorNode = new Node(2);
        Rendezvous rendezvous = mock(Rendezvous.class);
        final Bundle primaryBundle = mock(Bundle.class);
        final Bundle mirrorBundle = mock(Bundle.class);
        when(primaryBundle.getId()).thenReturn(primaryNode);
        when(mirrorBundle.getId()).thenReturn(mirrorNode);
        UUID channelId = UUID.randomUUID();
        final int maxSegmentSize = 1024 * 1024;

        primaryRoot = File.createTempFile("primary", ".channel");
        primaryRoot.delete();
        primaryRoot.mkdirs();
        primaryRoot.deleteOnExit();

        mirrorRoot = File.createTempFile("mirror", ".channel");
        mirrorRoot.delete();
        mirrorRoot.mkdirs();
        mirrorRoot.deleteOnExit();

        final Spindle spindle = new Spindle(primaryBundle);

        final Replicator primaryReplicator = new Replicator(primaryBundle,
                                                            mirrorNode,
                                                            rendezvous);
        final Replicator mirrorReplicator = new Replicator(mirrorBundle);

        EventChannel primaryEventChannel = new EventChannel(Role.PRIMARY,
                                                            channelId,
                                                            primaryRoot,
                                                            maxSegmentSize,
                                                            primaryReplicator);

        EventChannel mirrorEventChannel = new EventChannel(Role.MIRROR,
                                                           channelId,
                                                           mirrorRoot,
                                                           maxSegmentSize, null);
        when(primaryBundle.eventChannelFor(channelId)).thenReturn(primaryEventChannel);
        when(mirrorBundle.eventChannelFor(channelId)).thenReturn(mirrorEventChannel);

        SocketOptions socketOptions = new SocketOptions();

        ServerSocketChannelHandler spindleHandler = new ServerSocketChannelHandler(
                                                                                   "spindle handler",
                                                                                   socketOptions,
                                                                                   new InetSocketAddress(
                                                                                                         "127.0.0.1",
                                                                                                         0),
                                                                                   Executors.newFixedThreadPool(5),
                                                                                   new CommunicationsHandlerFactory() {

                                                                                       @Override
                                                                                       public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
                                                                                           return spindle;
                                                                                       }
                                                                                   });
        ServerSocketChannelHandler replicatorHandler = new ServerSocketChannelHandler(
                                                                                      "replicator handler",
                                                                                      socketOptions,
                                                                                      new InetSocketAddress(
                                                                                                            "127.0.0.1",
                                                                                                            0),
                                                                                      Executors.newFixedThreadPool(5),
                                                                                      new CommunicationsHandlerFactory() {

                                                                                          @Override
                                                                                          public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
                                                                                              return mirrorReplicator;
                                                                                          }
                                                                                      });
        spindleHandler.start();
        replicatorHandler.start();

        spindleHandler.connectTo(replicatorHandler.getLocalAddress(),
                                 primaryReplicator);

        int magic = 666;
        SocketOptions options = new SocketOptions();
        SocketChannel outbound = SocketChannel.open();
        options.setTimeout(1000);
        options.configure(outbound.socket());
        outbound.configureBlocking(true);
        outbound.connect(spindleHandler.getLocalAddress());
        assertTrue(outbound.isConnected());
        ByteBuffer buffer = ByteBuffer.allocate(Spindle.HANDSHAKE_SIZE);
        buffer.putInt(Spindle.MAGIC);
        producerNode.serialize(buffer);
        buffer.flip();
        outbound.write(buffer);
        byte[] payload = String.format("%s Give me Slack, or give me Food, or Kill me %s",
                                       channelId, channelId).getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(magic, payloadBuffer);
        int batchSize = 100;
        int batchLength = batchSize * event.totalSize();
        int totalSize = 1024 * 1024 * 2;
        long timestamp = 0;
        for (int currentSize = 0; currentSize < totalSize; currentSize += batchLength) {
            timestamp += batchSize;
            System.out.println("Writing out batch: " + timestamp / batchSize);
            BatchHeader header = new BatchHeader(producerNode, batchLength,
                                                 magic, channelId, timestamp);
            header.rewind();
            header.write(outbound);
            for (int j = 0; j < batchSize; j++) {
                event.rewind();
                event.write(outbound);
            }
        }
        outbound.close();
    }

    @After
    public void teardown() {
        if (primaryRoot != null) {
            Utils.deleteDirectory(primaryRoot);
        }
        if (mirrorRoot != null) {
            Utils.deleteDirectory(mirrorRoot);
        }
    }
}
