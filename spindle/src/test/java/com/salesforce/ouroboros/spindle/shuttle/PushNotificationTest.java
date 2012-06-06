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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.EventSpan;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.shuttle.PushNotificationContext.PushNotificationFSM;
import com.salesforce.ouroboros.testUtils.Util;

/**
 * @author hhildebrand
 * 
 */
public class PushNotificationTest {

    @Test
    public void testPush() throws IOException, InterruptedException {
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        final Node partner = new Node(1);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);
        final ArrayList<EventSpan> spans = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            spans.add(new EventSpan(i, UUID.randomUUID(), i, i));
        }

        when(handler.getChannel()).thenReturn(socketChannel);

        final AtomicInteger written = new AtomicInteger(0);
        Answer<Integer> firstWrite = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                written.incrementAndGet();
                return 0;
            }
        };
        Answer<Integer> writeBatchBytes = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                EventSpan pushed = new EventSpan(buffer);
                assertEquals(spans.get(written.get() - 1), pushed);
                written.incrementAndGet();
                return EventSpan.BYTE_SIZE;
            }
        };

        doAnswer(firstWrite).doAnswer(writeBatchBytes).doAnswer(writeBatchBytes).doAnswer(writeBatchBytes).when(socketChannel).write(isA(ByteBuffer.class));

        PushNotification controller = new PushNotification(bundle);
        controller.accept(handler, partner);

        assertEquals(PushNotificationFSM.Waiting, controller.getState());

        for (EventSpan span : spans) {
            controller.push(span);
        }
        Util.waitFor("First write never occurred", new Util.Condition() {
            @Override
            public boolean value() {
                return written.get() >= 1;
            }
        }, 2000, 100);
        controller.writeReady();
        controller.writeReady();
        controller.writeReady();
        Util.waitFor("Acknowledgement not written", new Util.Condition() {
            @Override
            public boolean value() {
                return written.get() == 4;
            }
        }, 2000, 100);
        verify(socketChannel, new Times(4)).write(isA(ByteBuffer.class));
    }
}
