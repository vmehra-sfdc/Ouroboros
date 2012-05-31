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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.shuttle.EventSpan;
import com.salesforce.ouroboros.spindle.shuttle.Flyer;
import com.salesforce.ouroboros.spindle.shuttle.PushResponse;
import com.salesforce.ouroboros.spindle.shuttle.PushResponse.Status;
import com.salesforce.ouroboros.spindle.shuttle.SpanHeader;
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
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(segment.transferTo(0, length, channel)).thenReturn((long) length);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        PushResponse response = flyer.push(channel,
                                           length + SpanHeader.HEADER_BYTE_SIZE);

        assertEquals(1024 + SpanHeader.HEADER_BYTE_SIZE, response.bytesWritten);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);
        verify(segment, new Times(1)).transferTo(isA(Long.class),
                                                 isA(Long.class), eq(channel));
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
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        long increment = 256L;
        when(segment.transferTo(0, 1024 - SpanHeader.HEADER_BYTE_SIZE, channel)).thenReturn(increment);
        when(segment.transferTo(increment, 1024 - increment, channel)).thenReturn(increment);
        when(segment.transferTo(increment * 2, 1024 - 2 * increment, channel)).thenReturn(increment);
        when(segment.transferTo(increment * 3, 1024 - 3 * increment, channel)).thenReturn(1024 - 3 * increment);
        when(
             segment.transferTo(1024 - SpanHeader.HEADER_BYTE_SIZE,
                                SpanHeader.HEADER_BYTE_SIZE, channel)).thenReturn((long) SpanHeader.HEADER_BYTE_SIZE);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        long maxBytes = 1024;

        PushResponse response = flyer.push(channel, maxBytes);
        assertEquals(SpanHeader.HEADER_BYTE_SIZE + increment,
                     response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, maxBytes);
        assertEquals(Status.CONTINUE, response.writeStatus);
        assertEquals(increment, response.bytesWritten);

        response = flyer.push(channel, maxBytes);
        assertEquals(Status.CONTINUE, response.writeStatus);
        assertEquals(increment, response.bytesWritten);

        response = flyer.push(channel, maxBytes);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);
        assertEquals(increment, response.bytesWritten);

        response = flyer.push(channel, maxBytes);
        assertEquals(Status.NO_SPAN, response.writeStatus);
        assertEquals(0, response.bytesWritten);

        verify(segment, new Times(4)).transferTo(isA(Long.class),
                                                 isA(Long.class), eq(channel));
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
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(
             segment.transferTo(0, length - SpanHeader.HEADER_BYTE_SIZE,
                                channel)).thenReturn(-1L);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        PushResponse response = flyer.push(channel, 1024);

        assertEquals(-1, response.bytesWritten);
        assertEquals(Status.SOCKET_CLOSED, response.writeStatus);
        verify(segment, new Times(1)).transferTo(isA(Long.class),
                                                 isA(Long.class), eq(channel));
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
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId, buffer.getLong());
                assertEquals(length, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader);
        when(
             segment.transferTo(0, length - SpanHeader.HEADER_BYTE_SIZE,
                                channel)).thenReturn((long) length
                                                             - SpanHeader.HEADER_BYTE_SIZE);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id);

        PushResponse response = flyer.push(channel, length);

        assertEquals(1024, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);
        verify(segment, new Times(1)).transferTo(isA(Long.class),
                                                 isA(Long.class), eq(channel));
    }

    @Test
    public void testMultiplePushFull() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId1 = 0x666;
        final long eventId2 = 0x777;
        final int length1 = 1024;
        final int length2 = 1024;
        flyer.deliver(new EventSpan(eventId1, segment, 0, length1));
        flyer.deliver(new EventSpan(eventId2, segment, 0, length2));
        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        assertFalse(id1.equals(id2));

        Answer<Long> writeHeader1 = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id1.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id1.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId1, buffer.getLong());
                assertEquals(length1, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };

        Answer<Long> writeHeader2 = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id2.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id2.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId2, buffer.getLong());
                assertEquals(length2, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader1).thenAnswer(writeHeader2);
        when(segment.transferTo(0, length1, channel)).thenReturn((long) length1);
        when(segment.transferTo(0, length2, channel)).thenReturn((long) length2);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id1).thenReturn(id2);

        int maxBytes = length1 + SpanHeader.HEADER_BYTE_SIZE;
        PushResponse response = flyer.push(channel, maxBytes);

        assertEquals(length1 + SpanHeader.HEADER_BYTE_SIZE,
                     response.bytesWritten);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);
        response = flyer.push(channel, maxBytes);

        assertEquals(length2 + SpanHeader.HEADER_BYTE_SIZE,
                     response.bytesWritten);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);
        verify(segment, new Times(2)).transferTo(isA(Long.class),
                                                 isA(Long.class), eq(channel));
    }

    @Test
    public void testMultiplePushPartial() throws Exception {
        Flyer flyer = new Flyer();
        SocketChannel channel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        EventChannel eventChannel = mock(EventChannel.class);
        final long eventId1 = 0x666;
        final long eventId2 = 0x777;
        final int length1 = 1024;
        final int length2 = 2048;
        flyer.deliver(new EventSpan(eventId1, segment, 0, length1));
        flyer.deliver(new EventSpan(eventId2, segment, 0, length2));
        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        assertFalse(id1.equals(id2));

        Answer<Long> writeHeader1 = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id1.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id1.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId1, buffer.getLong());
                assertEquals(length1, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };

        Answer<Long> writeHeader2 = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull(buffer);
                assertEquals(SpanHeader.MAGIC, buffer.getInt());
                assertEquals(id2.getLeastSignificantBits(), buffer.getLong());
                assertEquals(id2.getMostSignificantBits(), buffer.getLong());
                assertEquals(eventId2, buffer.getLong());
                assertEquals(length2, buffer.getInt());
                return (long) SpanHeader.HEADER_BYTE_SIZE;
            }
        };
        long increment = 256;
        when(channel.write(isA(ByteBuffer.class))).thenAnswer(writeHeader1).thenAnswer(writeHeader2);
        when(segment.transferTo(isA(Long.class), isA(Long.class), eq(channel))).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(0L).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment).thenReturn(increment);
        when(segment.getEventChannel()).thenReturn(eventChannel);
        when(eventChannel.getId()).thenReturn(id1).thenReturn(id2);

        int totalBytes = length1 + length2 + (2 * SpanHeader.HEADER_BYTE_SIZE);

        PushResponse response = flyer.push(channel, totalBytes);
        assertEquals(increment + SpanHeader.HEADER_BYTE_SIZE,
                     response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(SpanHeader.HEADER_BYTE_SIZE, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.CONTINUE, response.writeStatus);

        response = flyer.push(channel, totalBytes);
        assertEquals(increment, response.bytesWritten);
        assertEquals(Status.SPAN_COMPLETE, response.writeStatus);

        verify(segment, new Times(13)).transferTo(isA(Long.class),
                                                  isA(Long.class), eq(channel));
    }
}
