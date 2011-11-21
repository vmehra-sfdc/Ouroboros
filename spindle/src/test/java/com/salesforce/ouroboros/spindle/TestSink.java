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
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.SinkContext.SinkFSM;

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

        verify(socket, new Times(6)).read(isA(ByteBuffer.class));
        verify(segment, new Times(2)).transferFrom(isA(ReadableByteChannel.class),
                                                   isA(Long.class),
                                                   isA(Long.class));
        assertEquals(SinkFSM.ReadHandshake, sink.getState());

    }
}
