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
package com.salesforce.ouroboros.spindle.flyer;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.testUtils.Util;

/**
 * @author hhildebrand
 * 
 */
public class FlyerTest {
    @Test
    public void testDelivery() throws Exception {
        Flyer flyer = new Flyer();
        @SuppressWarnings("unchecked")
        BlockingDeque<EventSpan> thread = (BlockingDeque<EventSpan>) Util.accessField("thread",
                                                                                      flyer);
        assertNotNull(thread);
        assertEquals(0, thread.size());
        EventSpan span = new EventSpan(0, null, 0, 0);
        flyer.deliver(span);
        assertEquals(1, thread.size());
        assertSame(span, thread.peek());
    }

    @Test
    public void testInitialPush() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId = 0x666;
        final int length = 1024;
        EventSpan span = new EventSpan(eventId, segment, 0, length);
        flyer.deliver(span);
        final UUID id = UUID.randomUUID();

        Answer<Long> writeHeader = new Answer<Long>() {

            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(Flyer.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) Flyer.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(segment.transferTo(0, length, channel)).thenReturn((long) length);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        long written = flyer.push(channel, length + Flyer.HEADER_BYTE_SIZE);

        assertEquals(1024 + Flyer.HEADER_BYTE_SIZE, written);
    }

    @Test
    public void testLongPush() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId = 0x666;
        final int length = 1024;
        EventSpan span = new EventSpan(eventId, segment, 0, length);
        flyer.deliver(span);
        final UUID id = UUID.randomUUID();

        Answer<Long> writeHeader = new Answer<Long>() {

            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(Flyer.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) Flyer.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        long increment = 256L;
        when(segment.transferTo(0, 1024 - Flyer.HEADER_BYTE_SIZE, channel)).thenReturn(increment);
        when(
             segment.transferTo(increment, 1024 - Flyer.HEADER_BYTE_SIZE
                                           - increment, channel)).thenReturn(increment);
        when(
             segment.transferTo(increment * 2, 1024 - Flyer.HEADER_BYTE_SIZE
                                               - 2 * increment, channel)).thenReturn(increment);
        when(
             segment.transferTo(increment * 3, 1024 - Flyer.HEADER_BYTE_SIZE
                                               - 3 * increment, channel)).thenReturn(1024
                                                                                             - Flyer.HEADER_BYTE_SIZE
                                                                                             - 3
                                                                                             * increment);
        when(
             segment.transferTo(1024 - Flyer.HEADER_BYTE_SIZE,
                                Flyer.HEADER_BYTE_SIZE, channel)).thenReturn((long) Flyer.HEADER_BYTE_SIZE);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        long maxBytes = 1024;
        long written = flyer.push(channel, maxBytes);
        written = flyer.push(channel, maxBytes);
        assertEquals(Flyer.HEADER_BYTE_SIZE, written);
        written = flyer.push(channel, maxBytes);
        assertEquals(0, written);
    }

    @Test
    public void testFailedPush() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId = 0x666;
        final int length = 1024;
        EventSpan span = new EventSpan(eventId, segment, 0, length);
        flyer.deliver(span);
        final UUID id = UUID.randomUUID();

        Answer<Long> writeHeader = new Answer<Long>() {

            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(Flyer.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) Flyer.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(segment.transferTo(0, length - Flyer.HEADER_BYTE_SIZE, channel)).thenReturn(-1L);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        long written = flyer.push(channel, 1024);

        assertEquals(-1, written);
    }

    @Test
    public void testPartialPush() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId = 0x666;
        final int length = 1024;
        EventSpan span = new EventSpan(eventId, segment, 0, length);
        flyer.deliver(span);
        final UUID id = UUID.randomUUID();

        Answer<Long> writeHeader = new Answer<Long>() {

            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(Flyer.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) Flyer.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(segment.transferTo(0, length - Flyer.HEADER_BYTE_SIZE, channel)).thenReturn((long) length - Flyer.HEADER_BYTE_SIZE);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        long written = flyer.push(channel, length);

        assertEquals(1024, written);
    }
}
