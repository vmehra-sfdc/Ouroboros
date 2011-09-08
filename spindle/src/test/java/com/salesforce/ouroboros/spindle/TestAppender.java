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
import static org.mockito.Mockito.doAnswer;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.AbstractAppender.State;

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
        server.socket().bind(new InetSocketAddress(0));
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
        EventHeader header = new EventHeader(payload.length, magic, channel,
                                             timestamp, Event.crc32(payload));
        when(bundle.eventChannelFor(eq(header))).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(writeSegment);
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
        assertEquals(channel, event.getChannel());
        assertEquals(timestamp, event.getId());
        assertEquals(payload.length, event.size());
        ByteBuffer writtenPayload = event.getPayload();
        for (byte b : payload) {
            assertEquals(b, writtenPayload.get());
        }

        verify(handler, new Times(3)).selectForRead();
        verify(bundle).eventChannelFor(eq(header));
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
        server.socket().bind(new InetSocketAddress(0));
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
        EventHeader header = new EventHeader(payload.length, magic, channel,
                                             timestamp, Event.crc32(payload));
        when(bundle.eventChannelFor(eq(header))).thenReturn(eventChannel);
        when(eventChannel.segmentFor(eq(header))).thenReturn(writeSegment);
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
        verify(bundle).eventChannelFor(eq(header));
        verify(eventChannel).segmentFor(eq(header));
        verify(eventChannel).isDuplicate(eq(header));
        verify(eventChannel).nextOffset();
        verifyNoMoreInteractions(handler, bundle, eventChannel);
    };

    @Test
    public void testMultiAppend() throws Exception {
        final File tmpFile = File.createTempFile("multi-append", ".tst");
        tmpFile.deleteOnExit();
        SocketChannelHandler<?> handler = mock(SocketChannelHandler.class);
        final EventChannel eventChannel = mock(EventChannel.class);
        final Segment segment = new Segment(tmpFile);
        when(eventChannel.segmentFor(isA(EventHeader.class))).thenReturn(segment);
        when(eventChannel.nextOffset()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return segment.size();
            }
        });
        final AtomicInteger counter = new AtomicInteger();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                counter.incrementAndGet();
                return null;
            }
        }).when(eventChannel).append(isA(EventHeader.class), isA(Long.class),
                                     eq(segment));
        Bundle bundle = new Bundle() {
            @Override
            public EventChannel eventChannelFor(EventHeader header) {
                return eventChannel;
            }

            @Override
            public void registerReplicator(Node id, Replicator replicator) {
                throw new UnsupportedOperationException();
            }
        };

        final AbstractAppender appender = new Appender(bundle);
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setSend_buffer_size(8);
        socketOptions.setReceive_buffer_size(8);
        socketOptions.setTimeout(100);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress(0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);
        appender.handleAccept(inbound, handler);

        final int msgCount = 666;
        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread appenderThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (msgCount != counter.get()) {
                    appender.handleRead(inbound);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                try {
                    barrier.await();
                } catch (Exception e) {
                    return;
                }
            }
        }, "Appender driver");
        appenderThread.start();

        byte[][] payload = new byte[msgCount][];

        for (int i = 0; i < msgCount; i++) {
            int magic = i;
            UUID channelTag = new UUID(0, i);
            long timestamp = i;
            payload[i] = ("Give me Slack, or give me Food, or Kill me #" + i).getBytes();
            ByteBuffer payloadBuffer = ByteBuffer.wrap(payload[i]);
            EventHeader header = new EventHeader(payload[i].length, magic,
                                                 channelTag, timestamp,
                                                 Event.crc32(payload[i]));
            header.rewind();
            header.write(outbound);
            outbound.write(payloadBuffer);
        }

        barrier.await(30, TimeUnit.SECONDS);

        outbound.close();
        segment.close();

        FileInputStream fis = new FileInputStream(tmpFile);
        FileChannel readSegment = fis.getChannel();

        for (int i = 0; i < msgCount; i++) {
            Event event = new Event(readSegment);
            assertTrue(event.validate());
            assertEquals(i, event.getMagic());
            assertEquals(new UUID(0, i), event.getChannel());
            assertEquals(i, event.getId());
            assertEquals(payload[i].length, event.size());
            ByteBuffer writtenPayload = event.getPayload();
            for (byte b : payload[i]) {
                assertEquals(b, writtenPayload.get());
            }
        }
        readSegment.close();
    }
}
