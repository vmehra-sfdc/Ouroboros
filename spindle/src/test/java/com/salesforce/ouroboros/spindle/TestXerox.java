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
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.Xerox.State;

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
        SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);
        Node node = new Node(0x1639, 0x1640, 0x1641);
        CountDownLatch latch = mock(CountDownLatch.class);

        final UUID id = UUID.randomUUID();
        final long prefix1 = 77L;
        final long prefix2 = 1056L;
        final long size1 = 2048;
        final long size2 = 1024;

        Answer<Integer> write1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getLong());
                assertEquals(id, new UUID(buffer.getLong(), buffer.getLong()));
                return 24;
            }
        };
        Answer<Integer> write2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getLong());
                assertEquals(prefix1, buffer.getLong());
                assertEquals(size1, buffer.getLong());
                return 24;
            }
        };
        Answer<Integer> write3 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Xerox.MAGIC, buffer.getLong());
                assertEquals(prefix2, buffer.getLong());
                assertEquals(size2, buffer.getLong());
                return 24;
            }
        };
        when(socket.write(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(write1).thenReturn(0).thenAnswer(write2).thenReturn(0).thenAnswer(write3).thenReturn(0);
        when(segment1.transferTo(0L, 1024L, socket)).thenReturn(1024L);
        when(segment1.transferTo(1024L, 1024L, socket)).thenReturn(1024L);
        when(segment2.transferTo(0L, 1024L, socket)).thenReturn(512L);
        when(segment2.transferTo(512L, 1024L, socket)).thenReturn(512L);
        LinkedList<Segment> segments = new LinkedList<Segment>();
        segments.push(segment2);
        segments.push(segment1);
        when(segment1.getPrefix()).thenReturn(prefix1);
        when(segment2.getPrefix()).thenReturn(prefix2);
        when(segment1.size()).thenReturn(size1);
        when(segment2.size()).thenReturn(size2);

        int transferSize = 1024;
        Xerox xerox = new Xerox(node, id, segments, transferSize);
        xerox.setLatch(latch);
        assertEquals(State.INITIALIZED, xerox.getState());
        xerox.handleConnect(socket, handler);
        assertEquals(State.HANDSHAKE, xerox.getState());
        verify(handler).selectForWrite();
        xerox.handleWrite(socket);
        assertEquals(State.SEND_HEADER, xerox.getState());
        xerox.handleWrite(socket);
        assertEquals(State.COPY, xerox.getState());
        xerox.handleWrite(socket);
        assertEquals(State.SEND_HEADER, xerox.getState());
        xerox.handleWrite(socket);
        assertEquals(State.COPY, xerox.getState());
        xerox.handleWrite(socket);
        assertEquals(State.FINISHED, xerox.getState());
        verify(latch).countDown();
    }
}
