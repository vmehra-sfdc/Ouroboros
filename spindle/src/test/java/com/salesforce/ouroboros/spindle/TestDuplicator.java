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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.spindle.Duplicator.State;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestDuplicator {

    private class Reader implements Runnable {
        private final SocketChannel inbound;
        private final ByteBuffer    offsetBuffer = ByteBuffer.allocate(8);
        long                        offset       = -1;
        final ByteBuffer            replicated;

        public Reader(final SocketChannel inbound, final int payloadLength) {
            super();
            this.inbound = inbound;
            replicated = ByteBuffer.allocate(EventHeader.HEADER_BYTE_SIZE
                                             + payloadLength);
        }

        @Override
        public void run() {
            try {
                readOffset();
                for (inbound.read(replicated); replicated.hasRemaining(); inbound.read(replicated)) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            replicated.flip();
        }

        private void readOffset() {
            try {
                for (inbound.read(offsetBuffer); offsetBuffer.hasRemaining(); inbound.read(offsetBuffer)) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                offsetBuffer.flip();
                offset = offsetBuffer.getLong();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Test
    public void testOutboundReplication() throws Exception {
        File tmpFile = File.createTempFile("outbound-replication", ".tst");
        tmpFile.deleteOnExit();
        Segment segment = new Segment(tmpFile);
        Bundle bundle = mock(Bundle.class);

        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        EventHeader event = new EventHeader(payload.length, magic, channel,
                                            timestamp, Event.crc32(payload));
        EventChannel eventChannel = mock(EventChannel.class);

        event.rewind();
        event.write(segment);
        segment.write(payloadBuffer);
        segment.force(false);
        SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);

        when(bundle.eventChannelFor(eq(event))).thenReturn(eventChannel);
        final Duplicator replicator = new Duplicator();
        assertEquals(State.WAITING, replicator.getState());
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
        outbound.configureBlocking(false);
        inbound.configureBlocking(false);
        Reader reader = new Reader(inbound, payload.length);
        Thread inboundRead = new Thread(reader, "Inbound read thread");
        inboundRead.start();
        replicator.handleConnect(outbound, handler);
        Util.waitFor("Never achieved WAITING state", new Util.Condition() {

            @Override
            public boolean value() {
                return State.WAITING == replicator.getState();
            }
        }, 1000L, 100L);
        replicator.replicate(eventChannel, 0, segment, event.totalSize());
        assertEquals(State.WRITE_OFFSET, replicator.getState());
        Util.waitFor("Never achieved WAITING state", new Util.Condition() {

            @Override
            public boolean value() {
                replicator.handleWrite(outbound);
                return State.WAITING == replicator.getState();
            }
        }, 1000L, 100L);
        inboundRead.join(4000);
        assertTrue(reader.replicated.hasRemaining());
        assertEquals(0, reader.offset);
        Event replicatedEvent = new Event(reader.replicated);
        assertEquals(event.size(), replicatedEvent.size());
        assertEquals(event.getMagic(), replicatedEvent.getMagic());
        assertEquals(event.getCrc32(), replicatedEvent.getCrc32());
        assertEquals(event.getTimestamp(), replicatedEvent.getTimestamp());
        assertTrue(replicatedEvent.validate());
        verify(eventChannel).commit(0);
    }

    @Test
    public void testReplicationLoop() throws Exception {
        SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);

        File inboundTmpFile = File.createTempFile("inbound-replication", ".tst");
        inboundTmpFile.deleteOnExit();
        Segment inboundSegment = new Segment(inboundTmpFile);
        EventChannel inboundEventChannel = mock(EventChannel.class);
        Bundle inboundBundle = mock(Bundle.class);
        final ReplicatingAppender inboundReplicator = new ReplicatingAppender(
                                                                              inboundBundle);
        EventChannel outboundEventChannel = mock(EventChannel.class);

        File tmpOutboundFile = File.createTempFile("outbound", ".tst");
        tmpOutboundFile.deleteOnExit();
        Segment outboundSegment = new Segment(tmpOutboundFile);

        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        EventHeader outboundEvent = new EventHeader(payload.length, magic,
                                                    channel, timestamp,
                                                    Event.crc32(payload));

        outboundEvent.rewind();
        outboundEvent.write(outboundSegment);
        outboundSegment.write(payloadBuffer);
        outboundSegment.force(false);
        long offset = 0L;

        when(inboundBundle.eventChannelFor(eq(outboundEvent))).thenReturn(inboundEventChannel);
        when(inboundEventChannel.segmentFor(offset)).thenReturn(inboundSegment);
        when(inboundEventChannel.isNextAppend(offset)).thenReturn(true);

        final Duplicator outboundDuplicator = new Duplicator();
        assertEquals(State.WAITING, outboundDuplicator.getState());
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
        outbound.configureBlocking(false);
        inbound.configureBlocking(false);

        inboundReplicator.handleAccept(inbound, handler);
        inboundReplicator.handleRead(inbound);
        assertEquals(AbstractAppender.State.READ_OFFSET,
                     inboundReplicator.getState());

        Runnable reader = new Runnable() {
            @Override
            public void run() {
                while (AbstractAppender.State.ACCEPTED != inboundReplicator.getState()) {
                    inboundReplicator.handleRead(inbound);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        };
        Thread inboundRead = new Thread(reader, "Inbound read thread");
        inboundRead.start();

        outboundDuplicator.handleConnect(outbound, handler);
        outboundDuplicator.replicate(outboundEventChannel, 0, outboundSegment,
                                     outboundEvent.totalSize());
        assertEquals(State.WRITE_OFFSET, outboundDuplicator.getState());
        Util.waitFor("Never achieved WAITING state", new Util.Condition() {
            @Override
            public boolean value() {
                outboundDuplicator.handleWrite(outbound);
                return State.WAITING == outboundDuplicator.getState();
            }
        }, 1000L, 100L);
        inboundRead.join(4000);

        assertEquals(AbstractAppender.State.ACCEPTED,
                     inboundReplicator.getState());

        inboundSegment.close();
        outboundSegment.close();

        Segment segment = new Segment(inboundTmpFile);
        assertTrue("Nothing written to inbound segment", segment.size() > 0);
        Event replicatedEvent = new Event(segment);
        assertEquals(outboundEvent.size(), replicatedEvent.size());
        assertEquals(outboundEvent.getMagic(), replicatedEvent.getMagic());
        assertEquals(outboundEvent.getCrc32(), replicatedEvent.getCrc32());
        assertEquals(outboundEvent.getTimestamp(), replicatedEvent.getTimestamp());
        assertTrue(replicatedEvent.validate());
    }
}
