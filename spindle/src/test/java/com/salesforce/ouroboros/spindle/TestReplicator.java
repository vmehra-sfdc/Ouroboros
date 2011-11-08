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
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Replicator.State;
import com.salesforce.ouroboros.util.Rendezvous;

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
        Rendezvous rendezvous = mock(Rendezvous.class);
        final Node node = new Node(0x1639, 0x1640, 0x1650);

        Replicator replicator = new Replicator(bundle, node, rendezvous);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putInt(Replicator.MAGIC);
                node.serialize(buffer);
                return Replicator.HANDSHAKE_SIZE;
            }
        }).when(socketChannel).read(isA(ByteBuffer.class));

        assertEquals(State.INITIAL, replicator.getState());
        replicator.bindTo(node);
        replicator.accept(handler);
        assertEquals(State.ESTABLISHED, replicator.getState());
        verify(handler).selectForRead();
    }

    @Test
    public void testOutboundEstablish() throws Exception {
        Bundle bundle = mock(Bundle.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(socketChannel);
        final Node node = new Node(0x1639, 0x1640, 0x1650);
        Rendezvous rendezvous = mock(Rendezvous.class);
        when(bundle.getId()).thenReturn(node);

        Replicator replicator = new Replicator(bundle, node, rendezvous);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Replicator.MAGIC, buffer.getInt());
                assertEquals(node, new Node(buffer));
                return Replicator.HANDSHAKE_SIZE;
            }
        }).when(socketChannel).write(isA(ByteBuffer.class));

        assertEquals(State.INITIAL, replicator.getState());
        replicator.connect(handler);
        assertEquals(State.OUTBOUND_HANDSHAKE, replicator.getState());
        verify(handler).selectForWrite();
        replicator.writeReady();
        assertEquals(State.ESTABLISHED, replicator.getState());
        verify(handler).selectForRead();
    }
}
