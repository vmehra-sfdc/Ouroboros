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
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.SubscriptionEvent;
import com.salesforce.ouroboros.spindle.shuttle.SubscriptionManagementContext.SubscriptionManagementFSM;

/**
 * @author hhildebrand
 * 
 */
public class SubscriptionManagementTest {
    @Test
    public void testAck() throws Exception {
        SocketChannel inbound = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(inbound);
        PushNotification notifier = mock(PushNotification.class);
        SubscriptionManagement sm = new SubscriptionManagement(new Node(0),
                                                               notifier);
        assertEquals(SubscriptionManagementFSM.Suspended, sm.getState());
        sm.connect(handler, new Node(1));
        assertEquals(SubscriptionManagementFSM.ReadMessage, sm.getState());
        final SubscriptionEvent event1 = new SubscriptionEvent(
                                                               UUID.randomUUID(),
                                                               true);
        final SubscriptionEvent event2 = new SubscriptionEvent(
                                                               UUID.randomUUID(),
                                                               false);

        Answer<Integer> writeEvent1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                event1.serializeOn(buffer);
                return SubscriptionEvent.BYTE_SIZE;
            }
        };

        Answer<Integer> readNothing = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                return 0;
            }
        };

        Answer<Integer> writeEvent2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                event2.serializeOn(buffer);
                return SubscriptionEvent.BYTE_SIZE;
            }
        };

        when(inbound.read(isA(ByteBuffer.class))).thenAnswer(writeEvent1).thenAnswer(readNothing).thenAnswer(writeEvent2).thenAnswer(readNothing);

        sm.readReady();
        verify(notifier).handle(event1);
        assertEquals(SubscriptionManagementFSM.ReadMessage, sm.getState());

        sm.readReady();

        sm.readReady();
        verify(notifier).handle(event2);
        assertEquals(SubscriptionManagementFSM.ReadMessage, sm.getState());

    }
}
