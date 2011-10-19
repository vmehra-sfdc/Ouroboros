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
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.AbstractAppender.State;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestAppender {

    @Test
    public void testAppend() throws Exception {
        final SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Bundle bundle = mock(Bundle.class);
        Acknowledger acknowledger = mock(Acknowledger.class);
        EventChannel eventChannel = mock(EventChannel.class);
        File tmpFile = File.createTempFile("append", ".tst");
        tmpFile.deleteOnExit();
        final Segment writeSegment = new Segment(tmpFile);
        final Appender appender = new Appender(bundle, acknowledger);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);

        appender.handleAccept(inbound, handler);
        assertEquals(State.READY, appender.getState());

        Node mirror = new Node(0x1638);
        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(magic, payloadBuffer);
        BatchHeader header = new BatchHeader(mirror, event.totalSize(), magic,
                                             channel, timestamp);
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(new AppendSegment(
                                                                               writeSegment,
                                                                               0));
        when(eventChannel.isDuplicate(eq(header))).thenReturn(false);
        header.rewind();
        header.write(outbound);

        Util.waitFor("Header has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.APPEND;
            }
        }, 1000, 100);

        event.rewind();
        event.write(outbound);

        Util.waitFor("Payload has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.READY;
            }
        }, 1000, 100);

        outbound.close();
        inbound.close();
        server.close();

        FileInputStream fis = new FileInputStream(tmpFile);
        FileChannel readSegment = fis.getChannel();
        Event inboundEvent = new Event(readSegment);
        readSegment.close();
        assertTrue(inboundEvent.validate());
        assertEquals(magic, inboundEvent.getMagic());
        assertEquals(payload.length, inboundEvent.size());
        ByteBuffer writtenPayload = inboundEvent.getPayload();
        for (byte b : payload) {
            assertEquals(b, writtenPayload.get());
        }

        verify(handler, new Times(3)).selectForRead();
        verify(bundle).eventChannelFor(channel);
        verify(eventChannel).append(isA(ReplicatedBatchHeader.class),
                                    eq(writeSegment), isA(Acknowledger.class));
        verify(eventChannel).segmentFor(eq(header));
        verify(eventChannel).isDuplicate(eq(header));
        verifyNoMoreInteractions(handler, bundle, eventChannel);
    }

    @Test
    public void testDuplicate() throws Exception {
        final SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Bundle bundle = mock(Bundle.class);
        Acknowledger acknowledger = mock(Acknowledger.class);
        EventChannel eventChannel = mock(EventChannel.class);
        File tmpFile = File.createTempFile("duplicate", ".tst");
        tmpFile.deleteOnExit();
        final Segment writeSegment = new Segment(tmpFile);
        final Appender appender = new Appender(bundle, acknowledger);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);

        appender.handleAccept(inbound, handler);
        assertEquals(State.READY, appender.getState());

        Node mirror = new Node(0x1638);
        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(magic, payloadBuffer);
        BatchHeader header = new BatchHeader(mirror, event.totalSize(), magic,
                                             channel, timestamp);
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(new AppendSegment(
                                                                               writeSegment,
                                                                               0));
        when(eventChannel.isDuplicate(eq(header))).thenReturn(true);
        header.rewind();
        header.write(outbound);

        Util.waitFor("Header has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.DEV_NULL;
            }
        }, 1000, 100);

        event.rewind();
        event.write(outbound);

        Util.waitFor("Payload has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.READY;
            }
        }, 1000, 100);

        outbound.close();
        inbound.close();
        server.close();

        assertEquals(0L, tmpFile.length());
        verify(handler, new Times(3)).selectForRead();
        verify(bundle).eventChannelFor(channel);
        verify(eventChannel).segmentFor(eq(header));
        verify(eventChannel).isDuplicate(eq(header));
        verifyNoMoreInteractions(handler, bundle, eventChannel);
    };
}
