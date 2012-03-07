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

import java.net.Socket;
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
import com.salesforce.ouroboros.spindle.WeaverCoordinator;
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
        WeaverCoordinator coordinator = mock(WeaverCoordinator.class);
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);
        Socket socket = mock(Socket.class);
        when(socket.getSendBufferSize()).thenReturn(1024);
        SocketChannel socketChannel = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(socketChannel);
        when(socketChannel.socket()).thenReturn(socket);
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
                return Sink.CHANNEL_COUNT_HEADER_SIZE;
            }
        };

        Answer<Integer> writeChannelHeader = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getInt());
                assertEquals(2, buffer.getInt());
                assertEquals(id, new UUID(buffer.getLong(), buffer.getLong()));
                return Sink.CHANNEL_HEADER_SIZE;
            }
        };
        Answer<Integer> writeSegmentHeader1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getInt());
                assertEquals(prefix1, buffer.getLong());
                assertEquals(size1, buffer.getLong());
                return Sink.SEGMENT_HEADER_SIZE;
            }
        };
        Answer<Integer> writeSegmentHeader2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getInt());
                assertEquals(prefix2, buffer.getLong());
                assertEquals(size2, buffer.getLong());
                return Sink.SEGMENT_HEADER_SIZE;
            }
        };
        Answer<Integer> readAck = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putInt(Xerox.MAGIC);
                return Sink.ACK_HEADER_SIZE;
            }
        };
        when(socketChannel.write(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(writeChannelCount).thenReturn(0).thenAnswer(writeChannelHeader).thenReturn(0).thenAnswer(writeSegmentHeader1).thenReturn(0).thenAnswer(writeSegmentHeader2).thenReturn(0);
        when(socketChannel.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(readAck);
        when(segment1.transferTo(0L, 1024L, socketChannel)).thenReturn(1024L);
        when(segment1.transferTo(1024L, 1024L, socketChannel)).thenReturn(1024L);
        when(segment2.transferTo(0L, 1024L, socketChannel)).thenReturn(512L);
        when(segment2.transferTo(512L, 1024L, socketChannel)).thenReturn(512L);
        when(segment1.getPrefix()).thenReturn(prefix1);
        when(segment2.getPrefix()).thenReturn(prefix2);
        when(segment1.size()).thenReturn(size1);
        when(segment2.size()).thenReturn(size2);
        when(channel.getSegmentStack()).thenReturn(segments);
        when(channel.getId()).thenReturn(id);

        Xerox xerox = new Xerox(fromNode, node, coordinator);
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
