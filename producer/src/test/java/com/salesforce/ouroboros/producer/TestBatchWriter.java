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
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.BatchWriterContext.BatchWriterFSM;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestBatchWriter {

    @Test
    public void testBatching() throws Exception {
        final String[] events = new String[] { "Give me Slack",
                "or give me food", "or kill me" };
        ByteBuffer[] payloads = new ByteBuffer[] {
                ByteBuffer.wrap(events[0].getBytes()),
                ByteBuffer.wrap(events[1].getBytes()),
                ByteBuffer.wrap(events[2].getBytes()) };
        final int[] crc32 = new int[] { Event.crc32(events[0].getBytes()),
                Event.crc32(events[1].getBytes()),
                Event.crc32(events[2].getBytes()) };
        long timestamp = System.currentTimeMillis();
        final UUID channel = UUID.randomUUID();
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel outbound = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(outbound);

        SortedMap<BatchIdentity, Batch> pending = new TreeMap<BatchIdentity, Batch>();

        BatchWriter batchWriter = new BatchWriter();
        assertEquals(BatchWriterFSM.Suspended, batchWriter.getState());
        batchWriter.connect(handler);
        assertEquals(BatchWriterFSM.Waiting, batchWriter.getState());
        Node mirror = new Node(0x1638);
        Batch batch = new Batch(mirror, channel, timestamp,
                                Arrays.asList(payloads));
        Answer<Integer> readBatchHeader = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                BatchHeader header = new BatchHeader(buffer);
                assertEquals(channel, header.getChannel());
                buffer.position(buffer.limit());
                return BatchHeader.HEADER_BYTE_SIZE;
            }
        };
        Answer<Integer> readEventHeader0 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                EventHeader header = new EventHeader(buffer);
                assertEquals(events[0].getBytes().length, header.size());
                assertEquals(crc32[0], header.getCrc32());
                buffer.position(buffer.limit());
                return EventHeader.HEADER_BYTE_SIZE;
            }
        };
        Answer<Integer> readPayload0 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                byte[] b = new byte[events[0].getBytes().length];
                buffer.get(b, 0, b.length);
                assertEquals(events[0], new String(b));
                return b.length;
            }
        };
        Answer<Integer> readEventHeader1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                EventHeader header = new EventHeader(buffer);
                assertEquals(events[1].getBytes().length, header.size());
                assertEquals(crc32[1], header.getCrc32());
                buffer.position(buffer.limit());
                return EventHeader.HEADER_BYTE_SIZE;
            }
        };
        Answer<Integer> readPayload1 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                byte[] b = new byte[events[1].getBytes().length];
                buffer.get(b, 0, b.length);
                assertEquals(events[1], new String(b));
                return b.length;
            }
        };
        Answer<Integer> readEventHeader2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                EventHeader header = new EventHeader(buffer);
                assertEquals(events[2].getBytes().length, header.size());
                assertEquals(crc32[2], header.getCrc32());
                buffer.position(buffer.limit());
                return EventHeader.HEADER_BYTE_SIZE;
            }
        };
        Answer<Integer> readPayload2 = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                byte[] b = new byte[events[2].getBytes().length];
                buffer.get(b, 0, b.length);
                assertEquals(events[2], new String(b));
                return b.length;
            }
        };
        when(outbound.write(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(readBatchHeader).thenAnswer(readEventHeader0).thenAnswer(readPayload0).thenAnswer(readEventHeader1).thenAnswer(readPayload1).thenAnswer(readEventHeader2).thenAnswer(readPayload2);

        batchWriter.push(batch, pending);
        assertEquals(1, pending.size());
        assertEquals(batch, pending.get(batch));
        assertEquals(BatchWriterFSM.WriteBatchHeader, batchWriter.getState());
        
        batchWriter.writeReady();
        assertEquals(BatchWriterFSM.Waiting, batchWriter.getState());
        verify(outbound, new Times(8)).write(isA(ByteBuffer.class));
    }
}
