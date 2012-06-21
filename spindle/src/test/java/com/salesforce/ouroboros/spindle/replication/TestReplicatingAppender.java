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
package com.salesforce.ouroboros.spindle.replication;

import static junit.framework.Assert.assertEquals;
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

import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.batch.BatchWriter;
import com.salesforce.ouroboros.spindle.AppendSegment;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.Segment.Mode;
import com.salesforce.ouroboros.spindle.source.AbstractAppenderContext.AbstractAppenderFSM;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestReplicatingAppender {

    @Test
    public void testInboundReplication() throws Exception {
        EventChannel eventChannel = mock(EventChannel.class);
        File tmpFile = File.createTempFile("inbound-replication", ".tst");
        tmpFile.deleteOnExit();
        Segment segment = new Segment(eventChannel, tmpFile, Mode.APPEND);
        @SuppressWarnings("unchecked")
        BatchWriter<BatchIdentity> acknowledger = mock(BatchWriter.class);

        int magic = BatchHeader.MAGIC;
        UUID channel = UUID.randomUUID();
        long sequenceNumber = System.currentTimeMillis();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(magic, payloadBuffer);
        Node mirror = new Node(0x1638);
        ReplicatedBatchHeader header = new ReplicatedBatchHeader(
                                                                 mirror,
                                                                 event.totalSize(),
                                                                 magic,
                                                                 channel,
                                                                 sequenceNumber,
                                                                 0, 0);
        payloadBuffer.clear();
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(bundle.getAcknowledger(mirror)).thenReturn(acknowledger);
        when(
             eventChannel.appendSegmentFor(header.getOffset(),
                                           header.getPosition())).thenReturn(new AppendSegment(
                                                                                               segment,
                                                                                               0,
                                                                                               0));
        SocketChannelHandler handler = mock(SocketChannelHandler.class);

        final ReplicatingAppender replicator = new ReplicatingAppender(bundle);
        SocketOptions options = new SocketOptions();
        options.setSend_buffer_size(4);
        options.setReceive_buffer_size(4);
        options.setTimeout(100);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        final SocketChannel outbound = SocketChannel.open();
        options.configure(outbound.socket());
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        options.configure(inbound.socket());
        inbound.configureBlocking(true);

        assertTrue(inbound.isConnected());
        outbound.configureBlocking(true);
        inbound.configureBlocking(false);
        when(handler.getChannel()).thenReturn(inbound);

        replicator.accept(handler);
        assertEquals(AbstractAppenderFSM.Ready, replicator.getState());
        replicator.readReady();
        assertEquals(AbstractAppenderFSM.ReadBatchHeader, replicator.getState());
        header.rewind();
        header.write(outbound);
        replicator.readReady();
        assertEquals(AbstractAppenderFSM.Append, replicator.getState());
        event.rewind();
        event.write(outbound);
        replicator.readReady();
        assertEquals(AbstractAppenderFSM.Ready, replicator.getState());

        segment = new Segment(eventChannel, tmpFile, Mode.READ);
        Event replicatedEvent = new Event(segment);
        assertEquals(event.size(), replicatedEvent.size());
        assertEquals(event.getMagic(), replicatedEvent.getMagic());
        assertEquals(event.getCrc32(), replicatedEvent.getCrc32());
        assertTrue(replicatedEvent.validate());
        verify(eventChannel).append(header, 0L, segment);
        verify(acknowledger).send(new BatchIdentity(channel, sequenceNumber));
    }
}
