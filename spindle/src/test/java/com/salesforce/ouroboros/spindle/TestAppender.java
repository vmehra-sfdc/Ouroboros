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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
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
        final SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);
        Bundle bundle = mock(Bundle.class);
        EventChannel eventChannel = mock(EventChannel.class);
        File tmpFile = File.createTempFile("append", ".tst");
        tmpFile.deleteOnExit();
        final Segment writeSegment = new Segment(tmpFile);
        final AbstractAppender appender = new Appender(bundle);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);

        appender.handleAccept(inbound, handler);
        assertEquals(State.ACCEPTED, appender.getState());

        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        BatchHeader header = new BatchHeader(payload.length, magic, channel,
                                             timestamp);
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(new AppendSegment(
                                                                               writeSegment,
                                                                               0));
        when(eventChannel.isDuplicate(eq(header))).thenReturn(false);
        long offset = 0L;
        when(eventChannel.nextOffset()).thenReturn(offset);
        header.rewind();
        header.write(outbound);

        Util.waitFor("Header has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.APPEND;
            }
        }, 1000, 100);

        outbound.write(payloadBuffer);

        Util.waitFor("Payload has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.ACCEPTED;
            }
        }, 1000, 100);

        outbound.close();
        inbound.close();
        server.close();

        FileInputStream fis = new FileInputStream(tmpFile);
        FileChannel readSegment = fis.getChannel();
        Event event = new Event(readSegment);
        readSegment.close();
        assertTrue(event.validate());
        assertEquals(magic, event.getMagic());
        assertEquals(payload.length, event.size());
        ByteBuffer writtenPayload = event.getPayload();
        for (byte b : payload) {
            assertEquals(b, writtenPayload.get());
        }

        verify(handler, new Times(3)).selectForRead();
        verify(bundle).eventChannelFor(channel);
        verify(eventChannel).append(header, offset, writeSegment);
        verify(eventChannel).segmentFor(eq(header));
        verify(eventChannel).isDuplicate(eq(header));
        verify(eventChannel).nextOffset();
        verifyNoMoreInteractions(handler, bundle, eventChannel);
    }

    @Test
    public void testDuplicate() throws Exception {
        final SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);
        Bundle bundle = mock(Bundle.class);
        EventChannel eventChannel = mock(EventChannel.class);
        File tmpFile = File.createTempFile("duplicate", ".tst");
        tmpFile.deleteOnExit();
        final Segment writeSegment = new Segment(tmpFile);
        final AbstractAppender appender = new Appender(bundle);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress("127.0.0.1", 0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);

        appender.handleAccept(inbound, handler);
        assertEquals(State.ACCEPTED, appender.getState());

        int magic = 666;
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        BatchHeader header = new BatchHeader(payload.length, magic, channel,
                                             timestamp);
        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(new AppendSegment(
                                                                               writeSegment,
                                                                               0));
        when(eventChannel.isDuplicate(eq(header))).thenReturn(true);
        when(eventChannel.nextOffset()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return writeSegment.size();
            }
        });
        header.rewind();
        header.write(outbound);

        Util.waitFor("Header has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.DEV_NULL;
            }
        }, 1000, 100);

        outbound.write(payloadBuffer);

        Util.waitFor("Payload has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                appender.handleRead(inbound);
                return appender.getState() == State.ACCEPTED;
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
        verify(eventChannel).nextOffset();
        verifyNoMoreInteractions(handler, bundle, eventChannel);
    };
}
