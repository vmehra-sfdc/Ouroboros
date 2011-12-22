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
package com.salesforce.ouroboros.spindle.transfer;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.transfer.XeroxContext.XeroxFSM;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestXerox {

    @Test
    public void testCopy() throws Exception {
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);
        SocketChannel socket = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(socket);
        Node fromNode = new Node(0, 0, 0);
        Node node = new Node(0x1639, 0x1640, 0x1641);
        Rendezvous rendezvous = mock(Rendezvous.class);
        EventChannel channel = mock(EventChannel.class);
        LinkedList<Segment> segments = new LinkedList<Segment>();
        segments.push(segment2);
        segments.push(segment1);

        final UUID id = UUID.randomUUID();
        final long prefix1 = 77L;
        final long prefix2 = 1056L;
        final long size1 = 2048;
        final long size2 = 1024;

        Answer<Integer> writeChannelCount = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getInt());
                assertEquals(1, buffer.getInt());
                return 8;
            }
        };

        Answer<Integer> writeHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getInt());
                assertEquals(2, buffer.getInt());
                assertEquals(id, new UUID(buffer.getLong(), buffer.getLong()));
                return 24;
            }
        };
        Answer<Integer> writeHeader1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getLong());
                assertEquals(prefix1, buffer.getLong());
                assertEquals(size1, buffer.getLong());
                return 24;
            }
        };
        Answer<Integer> writeHeader2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getLong());
                assertEquals(prefix2, buffer.getLong());
                assertEquals(size2, buffer.getLong());
                return 24;
            }
        };
        Answer<Integer> readAck = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putInt(Xerox.MAGIC);
                return 4;
            }
        };
        when(socket.write(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(writeChannelCount).thenReturn(0).thenAnswer(writeHandshake).thenReturn(0).thenAnswer(writeHeader1).thenReturn(0).thenAnswer(writeHeader2).thenReturn(0);
        when(socket.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(readAck);
        when(segment1.transferTo(0L, 1024L, socket)).thenReturn(1024L);
        when(segment1.transferTo(1024L, 1024L, socket)).thenReturn(1024L);
        when(segment2.transferTo(0L, 1024L, socket)).thenReturn(512L);
        when(segment2.transferTo(512L, 1024L, socket)).thenReturn(512L);
        when(segment1.getPrefix()).thenReturn(prefix1);
        when(segment2.getPrefix()).thenReturn(prefix2);
        when(segment1.size()).thenReturn(size1);
        when(segment2.size()).thenReturn(size2);
        when(channel.getSegmentStack()).thenReturn(segments);
        when(channel.getId()).thenReturn(id);

        int transferSize = 1024;
        Xerox xerox = new Xerox(fromNode, node, transferSize);
        xerox.setRendezvous(rendezvous);
        xerox.addChannel(channel);
        assertEquals(XeroxFSM.Initial, xerox.getState());
        xerox.connect(handler);
        assertEquals(XeroxFSM.WriteChannelCount, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.Suspended, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.WriteChannelHeader, xerox.getState());
        verify(handler, new Times(3)).selectForWrite();
        xerox.writeReady();
        assertEquals(XeroxFSM.WriteSegmentHeader, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.CopySegment, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.WriteSegmentHeader, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.CopySegment, xerox.getState());
        xerox.writeReady();
        assertEquals(XeroxFSM.Finished, xerox.getState());
        xerox.readReady();
        assertEquals(XeroxFSM.Closed, xerox.getState());
        verify(rendezvous).meet();
    }
}
