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
package com.salesforce.ouroboros.spindle.source;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.AppendSegment;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.source.SpindleContext.SpindleFSM;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpindle {

    @Test
    public void testEstablish() throws Exception {
        Bundle bundle = mock(Bundle.class);
        when(bundle.getId()).thenReturn(new Node(0));
        Spindle spindle = new Spindle(bundle);
        assertEquals(SpindleFSM.Suspended, spindle.getState());
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel socketChannel = mock(SocketChannel.class);
        Segment segment = mock(Segment.class);
        final Node node = new Node(0x1638);

        EventChannel eventChannel = mock(EventChannel.class);
        Node mirror = new Node(0x1638);
        int magic = BatchHeader.MAGIC;
        final UUID channel = UUID.randomUUID();
        final long sequenceNumber = System.currentTimeMillis();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(magic, payloadBuffer);
        final BatchHeader header = new BatchHeader(mirror, event.totalSize(),
                                                   magic, channel,
                                                   sequenceNumber);
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.appendSegmentFor(eq(header))).thenReturn(new AppendSegment(
                                                                                     segment,
                                                                                     0,
                                                                                     0));
        when(eventChannel.isDuplicate(eq(header))).thenReturn(false);
        when(segment.transferFrom(socketChannel, 0, event.totalSize())).thenReturn(0L);
        header.rewind();

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.putInt(Spindle.MAGIC);
                node.serialize(buffer);
                return Spindle.HANDSHAKE_SIZE;
            }
        }).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.put(header.getBytes());
                return 100;
            }
        }).when(socketChannel).read(isA(ByteBuffer.class));

        when(handler.getChannel()).thenReturn(socketChannel);
        assertEquals(SpindleFSM.Suspended, spindle.getState());
        spindle.accept(handler);
        assertEquals(SpindleFSM.Handshake, spindle.getState());
        spindle.readReady();
        assertEquals(SpindleFSM.Established, spindle.getState());
        spindle.readReady();
        verify(handler, new Times(3)).selectForRead();
    }
}
