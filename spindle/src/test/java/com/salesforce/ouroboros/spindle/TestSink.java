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
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.SinkContext.SinkFSM;
import com.salesforce.ouroboros.spindle.XeroxContext.XeroxFSM;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSink {
    @Test
    public void testReadHandshake() throws Exception {
        Bundle bundle = mock(Bundle.class);
        SocketChannel socket = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(socket);

        Sink sink = new Sink(bundle);

        final UUID channelId = UUID.randomUUID();

        Answer<Integer> read = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Xerox.MAGIC);
                buffer.putLong(channelId.getMostSignificantBits());
                buffer.putLong(channelId.getLeastSignificantBits());
                return Xerox.BUFFER_SIZE;
            }
        };

        when(socket.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(read).thenReturn(0);

        sink.accept(handler);
        sink.readReady();
        assertEquals(SinkFSM.ReadHeader, sink.getState());

        verify(socket, new Times(3)).read(isA(ByteBuffer.class));
        verify(bundle).createEventChannelFor(channelId);
    }

    @Test
    public void testReadHeader() throws Exception {
        Bundle bundle = mock(Bundle.class);
        EventChannel channel = mock(EventChannel.class);
        Segment segment = mock(Segment.class);
        SocketChannel socket = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(socket);

        Sink sink = new Sink(bundle);

        final UUID channelId = UUID.randomUUID();
        final long prefix = 567L;
        final long bytesLeft = 6456L;

        Answer<Integer> handshakeRead = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Xerox.MAGIC);
                buffer.putLong(channelId.getMostSignificantBits());
                buffer.putLong(channelId.getLeastSignificantBits());
                return Xerox.BUFFER_SIZE;
            }
        };

        Answer<Integer> headerRead = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Xerox.MAGIC);
                buffer.putLong(prefix);
                buffer.putLong(bytesLeft);
                return Xerox.BUFFER_SIZE;
            }
        };
        when(bundle.createEventChannelFor(channelId)).thenReturn(channel);
        when(channel.segmentFor(prefix)).thenReturn(segment);
        when(socket.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(handshakeRead).thenReturn(0).thenAnswer(headerRead).thenReturn(0);

        sink.accept(handler);
        sink.readReady();
        sink.readReady();
        assertEquals(SinkFSM.Copy, sink.getState());

        verify(socket, new Times(4)).read(isA(ByteBuffer.class));
        verify(channel).segmentFor(prefix);

    }

    @Test
    public void testCopy() throws Exception {
        Bundle bundle = mock(Bundle.class);
        EventChannel channel = mock(EventChannel.class);
        Segment segment = mock(Segment.class);
        SocketChannel socket = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(socket);
        final String content = "Give me Slack, or give me Food, or Kill me";

        Sink sink = new Sink(bundle);

        final UUID channelId = UUID.randomUUID();
        final long prefix = 567L;
        final long bytesLeft = content.getBytes().length;

        Answer<Integer> handshakeRead = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Xerox.MAGIC);
                buffer.putLong(channelId.getMostSignificantBits());
                buffer.putLong(channelId.getLeastSignificantBits());
                return Xerox.BUFFER_SIZE;
            }
        };

        Answer<Integer> headerRead = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Xerox.MAGIC);
                buffer.putLong(prefix);
                buffer.putLong(bytesLeft);
                return Xerox.BUFFER_SIZE;
            }
        };

        when(bundle.createEventChannelFor(channelId)).thenReturn(channel);
        when(channel.segmentFor(prefix)).thenReturn(segment);
        when(socket.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(handshakeRead).thenReturn(0).thenAnswer(headerRead).thenReturn(0);
        when(segment.transferFrom(socket, 0, bytesLeft)).thenReturn(12L);
        when(segment.transferFrom(socket, 12, bytesLeft - 12)).thenReturn(bytesLeft - 12);

        sink.accept(handler);
        sink.readReady();
        sink.readReady();
        sink.readReady();
        sink.readReady();

        verify(socket, new Times(5)).read(isA(ByteBuffer.class));
        verify(segment, new Times(2)).transferFrom(isA(ReadableByteChannel.class),
                                                   isA(Long.class),
                                                   isA(Long.class));
        assertEquals(SinkFSM.ReadHandshake, sink.getState());

    }

    @Test
    public void testXeroxSink() throws Exception {
        SocketChannelHandler inboundHandler = mock(SocketChannelHandler.class);
        File inboundTmpFile = File.createTempFile("sink", ".tst");
        inboundTmpFile.delete();
        inboundTmpFile.deleteOnExit();
        Segment inboundSegment = new Segment(inboundTmpFile);
        EventChannel inboundEventChannel = mock(EventChannel.class);
        Bundle inboundBundle = mock(Bundle.class);

        long prefix = 0x1600;
        SocketChannelHandler outboundHandler = mock(SocketChannelHandler.class);
        File tmpOutboundFile = new File(Long.toHexString(prefix)
                                        + EventChannel.SEGMENT_SUFFIX);
        tmpOutboundFile.delete();
        tmpOutboundFile.deleteOnExit();
        Segment outboundSegment = new Segment(tmpOutboundFile);

        Node toNode = new Node(0, 0, 0);
        UUID channel = UUID.randomUUID();

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

        when(inboundHandler.getChannel()).thenReturn(inbound);
        when(outboundHandler.getChannel()).thenReturn(outbound);

        when(inboundBundle.createEventChannelFor(channel)).thenReturn(inboundEventChannel);
        when(inboundEventChannel.segmentFor(prefix)).thenReturn(inboundSegment);

        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(666, payloadBuffer);

        event.rewind();
        event.write(outboundSegment);
        outboundSegment.write(payloadBuffer);
        outboundSegment.force(false);

        CountDownLatch latch = mock(CountDownLatch.class);
        final Xerox xerox = new Xerox(
                                      toNode,
                                      channel,
                                      new LinkedList<Segment>(
                                                              Arrays.asList(outboundSegment)));
        xerox.setLatch(latch);
        
        final Sink sink = new Sink(inboundBundle);

        sink.accept(inboundHandler);
        xerox.connect(outboundHandler);

        Runnable reader = new Runnable() {
            @Override
            public void run() {
                while (SinkFSM.ReadHeader != sink.getState()) {
                    sink.readReady();
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

        Util.waitFor("Never achieved Closed state", new Util.Condition() {
            @Override
            public boolean value() {
                if (XeroxFSM.Closed == xerox.getState()) {
                    return true;
                }
                xerox.writeReady();
                return false;
            }
        }, 1000L, 100L);
        inboundRead.join(4000);
    }
}
