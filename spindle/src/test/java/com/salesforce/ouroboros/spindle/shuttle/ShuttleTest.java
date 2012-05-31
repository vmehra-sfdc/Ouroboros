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
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Matchers.isA;
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
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.shuttle.PushResponse.Status;
import com.salesforce.ouroboros.spindle.shuttle.ShuttleContext.ShuttleFSM;

/**
 * @author hhildebrand
 * 
 */
public class ShuttleTest {

    @Test
    public void testHandshake() throws Exception {
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        Node self = new Node(0);
        final Node consumer = new Node(1);
        when(channel.read(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(new Answer<Integer>() {
                                                                               @Override
                                                                               public Integer answer(InvocationOnMock invocation)
                                                                                                                                 throws Throwable {
                                                                                   ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                                                                                   assertNotNull("null buffer",
                                                                                                 buffer);
                                                                                   consumer.serialize(buffer);
                                                                                   return Node.BYTE_LENGTH;
                                                                               }
                                                                           });
        Shuttle shuttle = new Shuttle(self);
        assertEquals("not in the suspended state", ShuttleFSM.Suspended,
                     shuttle.getState());
        shuttle.accept(handler);
        assertEquals("not in the handshake state", ShuttleFSM.Handshake,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not complete handshake", ShuttleFSM.Established,
                     shuttle.getState());
    }

    @Test
    public void testReadWeft() throws Exception {
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        Flyer flyer = mock(Flyer.class);
        Node self = new Node(0);
        final Node consumer = new Node(1);
        final UUID clientId = UUID.randomUUID();
        final int packetSize = 1024;
        PushResponse response = new PushResponse(0, Status.CONTINUE);
        when(flyer.push(channel, packetSize)).thenReturn(response);
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.setClientId(clientId);
                header.setPacketSize(packetSize);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return Node.BYTE_LENGTH;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).thenReturn(0);

        Shuttle shuttle = new Shuttle(self);
        shuttle.addSubscription(clientId, flyer);

        shuttle.accept(handler);
        assertEquals("Did not complete handshake", ShuttleFSM.Established,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not attemp to read weft", ShuttleFSM.ReadWeft,
                     shuttle.getState());

        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());

    }

    @Test
    public void testWriteSpan() throws Exception {
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        Flyer flyer = mock(Flyer.class);
        Node self = new Node(0);
        final Node consumer = new Node(1);
        final UUID clientId = UUID.randomUUID();
        final int packetSize = 1024;
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.setClientId(clientId);
                header.setPacketSize(packetSize);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return Node.BYTE_LENGTH;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).thenReturn(0);

        when(flyer.push(channel, packetSize)).thenReturn(new PushResponse(
                                                                          0,
                                                                          Status.CONTINUE)).thenReturn(new PushResponse(
                                                                                                                        256,
                                                                                                                        Status.CONTINUE));
        when(flyer.push(channel, 768)).thenReturn(new PushResponse(
                                                                   256,
                                                                   Status.CONTINUE));
        when(flyer.push(channel, 512)).thenReturn(new PushResponse(
                                                                   256,
                                                                   Status.CONTINUE));
        when(flyer.push(channel, 256)).thenReturn(new PushResponse(
                                                                   256,
                                                                   Status.SPAN_COMPLETE));

        Shuttle shuttle = new Shuttle(self);
        shuttle.addSubscription(clientId, flyer);

        shuttle.accept(handler);

        shuttle.readReady();
        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not continue to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not write span", ShuttleFSM.Established,
                     shuttle.getState());

        verify(flyer, new Times(2)).push(channel, packetSize);
        verify(flyer).push(channel, 768);
        verify(flyer).push(channel, 512);
        verify(flyer).push(channel, 256);
    }

    @Test
    public void testBatching() throws Exception {
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel channel = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(channel);
        Flyer flyer = mock(Flyer.class);
        Node self = new Node(0);
        final Node consumer = new Node(1);
        final UUID clientId = UUID.randomUUID();
        final int packetSize = 1024;
        Answer<Integer> readHandshake = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                consumer.serialize(buffer);
                return Node.BYTE_LENGTH;
            }
        };
        Answer<Integer> readWeft = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertNotNull("null buffer", buffer);
                WeftHeader header = new WeftHeader();
                header.setClientId(clientId);
                header.setPacketSize(packetSize);
                header.getBytes().rewind();
                buffer.put(header.getBytes());
                return Node.BYTE_LENGTH;
            }
        };
        when(channel.read(isA(ByteBuffer.class))).thenAnswer(readHandshake).thenReturn(0).then(readWeft).then(readWeft).thenReturn(0);

        PushResponse writeComplete = new PushResponse(1024,
                                                      Status.SPAN_COMPLETE);
        when(flyer.push(channel, packetSize)).thenReturn(new PushResponse(
                                                                          0,
                                                                          Status.CONTINUE)).thenReturn(writeComplete).thenReturn(writeComplete);

        Shuttle shuttle = new Shuttle(self);
        shuttle.addSubscription(clientId, flyer);

        shuttle.accept(handler);

        shuttle.readReady();
        shuttle.readReady();
        assertEquals("Did not attempt to write span", ShuttleFSM.WriteSpan,
                     shuttle.getState());
        shuttle.writeReady();
        assertEquals("Did not write spans", ShuttleFSM.Established,
                     shuttle.getState());
        verify(flyer, new Times(2)).push(channel, packetSize);

    }
}
