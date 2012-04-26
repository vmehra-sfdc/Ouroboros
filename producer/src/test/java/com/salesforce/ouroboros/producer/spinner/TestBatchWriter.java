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
package com.salesforce.ouroboros.producer.spinner;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
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
import com.salesforce.ouroboros.Batch;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.spinner.BatchWriterContext.BatchWriterFSM;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.testUtils.Util.Condition;

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
        long sequenceNumber = System.currentTimeMillis();
        final UUID channel = UUID.randomUUID();
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel outbound = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(outbound);

        SortedMap<BatchIdentity, Batch> pending = new TreeMap<BatchIdentity, Batch>();

        final BatchWriter batchWriter = new BatchWriter(100, "test");
        assertEquals(BatchWriterFSM.Suspended, batchWriter.getState());
        batchWriter.connect(handler);
        assertEquals(BatchWriterFSM.Waiting, batchWriter.getState());
        Node mirror = new Node(0x1638);
        Batch batch = new Batch();
        batch.set(mirror, channel, sequenceNumber, Arrays.asList(payloads));
        Answer<Integer> readBatchHeader = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                int totalLength = buffer.remaining();
                BatchHeader header = BatchHeader.readFrom(buffer);
                assertEquals(channel, header.getChannel());
                return totalLength;
            }
        };
        Answer<Integer> readBatch = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                int totalLength = buffer.remaining();
                for (int i = 0; i < 3; i++) {
                    Event event = Event.readFrom(buffer);
                    byte[] buf = new byte[events[i].getBytes().length];
                    event.getPayload().get(buf);
                    String payload = new String(buf);
                    assertEquals(events[i].getBytes().length, event.size());
                    assertEquals(crc32[i], event.getCrc32());
                    assertTrue("event failed CRC check", event.validate());
                    assertEquals(String.format("unexpected event content '%s'",
                                               payload), events[i], payload);
                }
                assertEquals(0, buffer.remaining());
                return totalLength;
            }
        };
        when(outbound.write(isA(ByteBuffer.class))).thenReturn(0).thenAnswer(readBatchHeader).thenAnswer(readBatch);

        batchWriter.push(batch, pending);
        Util.waitFor("Never entered the WriteBatch state", new Condition() {
            @Override
            public boolean value() {
                return batchWriter.getState() == BatchWriterFSM.WriteBatch;
            }
        }, 2000, 100);
        batchWriter.writeReady();
        assertEquals(1, pending.size());
        assertEquals(batch, pending.get(batch));
        assertEquals(BatchWriterFSM.Waiting, batchWriter.getState());
        verify(outbound, new Times(3)).write(isA(ByteBuffer.class));
    }
}
