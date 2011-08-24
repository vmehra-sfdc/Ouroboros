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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.Replicator.State;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestReplicator {
    @Test
    public void testInboundEstablish() throws Exception {
        Bundle bundle = mock(Bundle.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);

        Replicator replicator = new Replicator(bundle);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Replicator.MAGIC);
                buffer.putLong(0x1638L);
                return 16;
            }
        }).when(socketChannel).read(isA(ByteBuffer.class));

        assertEquals(State.INITIAL, replicator.getState());
        replicator.handleAccept(socketChannel, handler);
        assertEquals(State.INBOUND_HANDSHAKE, replicator.getState());
        replicator.handleRead(socketChannel);
        assertEquals(State.ESTABLISHED, replicator.getState());
        verify(bundle).registerReplicator(0x1638L, replicator);
    }

    @Test
    public void testInboundEstablishError() throws Exception {
        Bundle bundle = mock(Bundle.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);

        Replicator replicator = new Replicator(bundle);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putLong(Replicator.MAGIC + 1);
                buffer.putLong(0x1638L);
                return 16;
            }
        }).when(socketChannel).read(isA(ByteBuffer.class));

        assertEquals(State.INITIAL, replicator.getState());
        replicator.handleAccept(socketChannel, handler);
        assertEquals(State.INBOUND_HANDSHAKE, replicator.getState());
        replicator.handleRead(socketChannel);
        assertEquals(State.ERROR, replicator.getState());
        verify(handler).close();
        verifyNoMoreInteractions(bundle);
    }

    @Test
    public void testOutboundEstablish() throws Exception {
        Bundle bundle = mock(Bundle.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);

        Replicator replicator = new Replicator(0x1638L, bundle);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Replicator.MAGIC, buffer.getLong());
                assertEquals(0x1638L, buffer.getLong());
                return 16;
            }
        }).when(socketChannel).write(isA(ByteBuffer.class));

        assertEquals(State.INITIAL, replicator.getState());
        replicator.handleConnect(socketChannel, handler);
        assertEquals(State.OUTBOUND_HANDSHAKE, replicator.getState());
        replicator.handleWrite(socketChannel);
        assertEquals(State.ESTABLISHED, replicator.getState());
        verify(bundle).registerReplicator(0x1638L, replicator);
    }
}
