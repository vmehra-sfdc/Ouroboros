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
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.eq;
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

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestReplicatingAppender {

    @Test
    public void testInboundReplication() throws Exception {
        File tmpFile = File.createTempFile("inbound-replication", ".tst");
        tmpFile.deleteOnExit();
        Segment segment = new Segment(tmpFile);

        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer offsetBuffer = ByteBuffer.allocate(8);
        long offset = 0L;
        offsetBuffer.putLong(offset);
        offsetBuffer.flip();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        EventHeader event = new EventHeader(payload.length, magic, channel,
                                            timestamp, Event.crc32(payload));
        payloadBuffer.clear();
        EventChannel eventChannel = mock(EventChannel.class);
        Bundle bundle = mock(Bundle.class);
        when(bundle.eventChannelFor(eq(event))).thenReturn(eventChannel);
        when(eventChannel.segmentFor(offset)).thenReturn(segment);
        when(eventChannel.isNextAppend(offset)).thenReturn(true);
        SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);

        final ReplicatingAppender replicator = new ReplicatingAppender(bundle);
        SocketOptions options = new SocketOptions();
        options.setSend_buffer_size(4);
        options.setReceive_buffer_size(4);
        options.setTimeout(100);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress(0));
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

        replicator.handleAccept(inbound, handler);
        assertEquals(AbstractAppender.State.ACCEPTED, replicator.getState());
        replicator.handleRead(inbound);
        assertEquals(AbstractAppender.State.READ_OFFSET, replicator.getState());

        Runnable reader = new Runnable() {
            @Override
            public void run() {
                while (AbstractAppender.State.ACCEPTED != replicator.getState()) {
                    replicator.handleRead(inbound);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        };
        Thread inboundRead = new Thread(reader, "Inbound read thread");
        inboundRead.start();
        outbound.write(offsetBuffer);
        event.write(outbound);
        outbound.write(payloadBuffer);
        inboundRead.join(4000);
        assertEquals(AbstractAppender.State.ACCEPTED, replicator.getState());

        segment = new Segment(tmpFile);
        Event replicatedEvent = new Event(segment);
        assertEquals(event.size(), replicatedEvent.size());
        assertEquals(event.getMagic(), replicatedEvent.getMagic());
        assertEquals(event.getCrc32(), replicatedEvent.getCrc32());
        assertEquals(event.getId(), replicatedEvent.getId());
        assertTrue(replicatedEvent.validate());
        verify(eventChannel).append(eq(event), eq(0L));
    }
}
