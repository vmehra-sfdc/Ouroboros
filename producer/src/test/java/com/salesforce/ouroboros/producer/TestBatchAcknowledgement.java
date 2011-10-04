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
package com.salesforce.ouroboros.producer;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.producer.BatchAcknowledgement.State;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestBatchAcknowledgement {
    @Test
    public void testAck() throws Exception {
        SocketChannel inbound = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Spinner spinner = mock(Spinner.class);
        BatchAcknowledgement ba = new BatchAcknowledgement(spinner);
        assertEquals(State.INITIALIZED, ba.getState());
        ba.handleConnect(inbound, handler);
        assertEquals(State.READ_ACK, ba.getState());
        final BatchIdentity ack1 = new BatchIdentity(UUID.randomUUID(),
                                                     System.currentTimeMillis());
        final BatchIdentity ack2 = new BatchIdentity(
                                                     UUID.randomUUID(),
                                                     System.currentTimeMillis() + 100);

        Answer<Integer> writeAck1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                ack1.serializeOn(buffer);
                return BatchIdentity.BYTE_SIZE;
            }
        };

        Answer<Integer> readNothing = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                return 0;
            }
        };

        Answer<Integer> writeAck2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                ack2.serializeOn(buffer);
                return BatchIdentity.BYTE_SIZE;
            }
        };

        when(inbound.read(isA(ByteBuffer.class))).thenAnswer(writeAck1).thenAnswer(readNothing).thenAnswer(writeAck2).thenAnswer(readNothing);

        ba.handleRead(inbound);
        verify(spinner).acknowledge(ack1);
        assertEquals(State.READ_ACK, ba.getState());

        ba.handleRead(inbound);
        verify(spinner).acknowledge(ack2);
        assertEquals(State.READ_ACK, ba.getState());

    }
}
