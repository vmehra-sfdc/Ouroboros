/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.spindle.shuttle;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.WeftHeader;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventSegment;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.shuttle.ShuttleContext.ShuttleFSM;

/**
 * @author hhildebrand
 * 
 */
public class ShuttleTest {

    @Test
    public void testHandshake() throws Exception {
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        final Node consumer = new Node(1);
        when(channel.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(new Answer<Integer>() {
                                                                               @Override
                                                                               public Integer answer(InvocationOnMock invocation)
                                                                                                                                 throws Throwable {
                                                                                   ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                                                                                   assertNotNull("null buffer",
                                                                                                 buffer);
                                                                                   consumer.serialize(buffer);
                                                                                   return Node.BYTE_LENGTH;
                                                                               }
                                                                           });
        Shuttle shuttle = new Shuttle(bundle);
        assertEquals("not in the suspended state", ShuttleFSM.Suspended,
                     shuttle.getState());
        shuttle.accept(handler);
        assertEquals("not in the handshake state", ShuttleFSM.Handshake,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not complete handshake", ShuttleFSM.Established,
                     shuttle.getState());
    }

    @Test
    public void testReadWeft() throws Exception {
        EventChannel eventChannel = mock(EventChannel.class);
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        final Node consumer = new Node(1);
        final UUID channelId = UUID.randomUUID();
        Segment segment = mock(Segment.class);
        final int position = 0x1638;
        final long eventId = 0x666;
        final int endpoint = position + 0x10;
        final int packetSize = 1024;
        when(bundle.eventChannelFor(channelId)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eventId)).thenReturn(new EventSegment(0,
                                                                           segment));
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.set(channelId, eventId, packetSize, position, endpoint);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return WeftHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).thenReturn(0);

        Shuttle shuttle = new Shuttle(bundle);

        shuttle.accept(handler);
        assertEquals("Did not complete handshake", ShuttleFSM.Established,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not attempt to read weft", ShuttleFSM.ReadWeft,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());

    }

    @Test
    public void testWriteSpan() throws Exception {
        EventChannel eventChannel = mock(EventChannel.class);
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        final Node consumer = new Node(1);
        final UUID channelId = UUID.randomUUID();
        Segment segment = mock(Segment.class);
        final int position = 0x1638;
        final long eventId = 0x666;
        final int endpoint = position + 1024;
        final int packetSize = 1024;
        when(bundle.eventChannelFor(channelId)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eventId)).thenReturn(new EventSegment(0,
                                                                           segment));
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.set(channelId, eventId, packetSize, position, endpoint);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return WeftHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).thenReturn(0);

        when(segment.transferTo(position, 1024, channel)).thenReturn(0L).thenReturn(256L);
        when(segment.transferTo(position + 256, 768, channel)).thenReturn(256L);
        when(segment.transferTo(position + 512, 512, channel)).thenReturn(256L);
        when(segment.transferTo(position + 768, 256, channel)).thenReturn(256L);

        Shuttle shuttle = new Shuttle(bundle);

        shuttle.accept(handler);

        shuttle.readReady();
        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not write span", ShuttleFSM.Established,
                     shuttle.getState());

        verify(segment, new Times(2)).transferTo(position, 1024, channel);
        verify(segment).transferTo(position + 256, 768, channel);
        verify(segment).transferTo(position + 512, 512, channel);
        verify(segment).transferTo(position + 768, 256, channel);
    }

    @Test
    public void testBatching() throws Exception {
        EventChannel eventChannel = mock(EventChannel.class);
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        final Node consumer = new Node(1);
        final UUID channelId = UUID.randomUUID();
        Segment segment = mock(Segment.class);
        final int position = 0x1638;
        final long eventId = 0x666;
        final int endpoint = position + 1024;
        final int packetSize = 1024;
        when(bundle.eventChannelFor(channelId)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eventId)).thenReturn(new EventSegment(0,
                                                                           segment));
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.set(channelId, eventId, packetSize, position, endpoint);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return WeftHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).then(readWeft).thenReturn(0);

        when(segment.transferTo(position, 1024, channel)).thenReturn(0L).thenReturn(1024L).thenReturn(1024L);

        Shuttle shuttle = new Shuttle(bundle);

        shuttle.accept(handler);

        shuttle.readReady();
        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not write spans", ShuttleFSM.Established,
                     shuttle.getState());
        verify(segment, new Times(2)).transferTo(position, 1024, channel);

    }
}
