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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Batch;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.spinner.BatchEventWriterContext.BatchEventWriterFSM;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.testUtils.Util.Condition;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestBatchEventWriter {

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
        final long sequenceNumber = System.currentTimeMillis();
        final UUID channel = UUID.randomUUID();
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel outbound = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(outbound);

        SortedMap<BatchIdentity, Batch> pending = new TreeMap<BatchIdentity, Batch>();

        final BatchEventWriter batchWriter = new BatchEventWriter(100, 5,
                                                                  "test");
        assertEquals(BatchEventWriterFSM.Suspended, batchWriter.getState());
        batchWriter.connect(handler);
        assertEquals(BatchEventWriterFSM.Waiting, batchWriter.getState());
        final Node mirror = new Node(0x1638);
        Batch batch = new Batch(mirror, channel, sequenceNumber,
                                Arrays.asList(payloads));
        Answer<Integer> readBatchHeader = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer[] buffers = (ByteBuffer[]) invocation.getArguments()[0];
                int offset = (Integer) invocation.getArguments()[1];
                int totalLength = buffers[offset].remaining();
                BatchHeader header = BatchHeader.readFrom(buffers[offset]);
                assertEquals(channel, header.getChannel());
                assertEquals(sequenceNumber, header.getSequenceNumber());
                assertEquals(mirror, header.getProducerMirror());
                assertEquals(BatchHeader.MAGIC, header.getMagic());
                return totalLength;
            }
        };
        Answer<Integer> readBatch = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer[] buffers = (ByteBuffer[]) invocation.getArguments()[0];
                int offset = (Integer) invocation.getArguments()[1];
                int totalLength = buffers[offset + 1].remaining();
                for (int i = 0; i < 3; i++) {
                    Event event = Event.readFrom(buffers[1]);
                    byte[] buf = new byte[events[i].getBytes().length];
                    event.getPayload().get(buf);
                    String payload = new String(buf);
                    assertEquals(events[i].getBytes().length, event.size());
                    assertEquals(crc32[i], event.getCrc32());
                    assertTrue("event failed CRC check", event.validate());
                    assertEquals(String.format("unexpected event content '%s'",
                                               payload), events[i], payload);
                }
                assertEquals(0, buffers[offset + 1].remaining());
                return totalLength;
            }
        };
        ByteBuffer[] temp = new ByteBuffer[0];
        when(
             outbound.write(any(temp.getClass()), any(Integer.class),
                            any(Integer.class))).thenReturn(0L).thenReturn(0L).thenAnswer(readBatchHeader).thenAnswer(readBatch);

        batchWriter.push(batch, pending);
        Util.waitFor("Never entered the WriteBatch state", new Condition() {
            @Override
            public boolean value() {
                return batchWriter.getState() == BatchEventWriterFSM.WriteBatch;
            }
        }, 2000, 100);
        batchWriter.writeReady();
        assertEquals(1, pending.size());
        assertEquals(batch, pending.get(batch));
        assertEquals(BatchEventWriterFSM.Waiting, batchWriter.getState());
        verify(outbound, new Times(4)).write(any(temp.getClass()),
                                             any(Integer.class),
                                             any(Integer.class));
    }

    @Test
    public void testCompact() throws Exception {
        final String[] events = new String[] { "Give me Slack",
                "or give me food", "or kill me" };
        ByteBuffer[] payloads = new ByteBuffer[] {
                ByteBuffer.wrap(events[0].getBytes()),
                ByteBuffer.wrap(events[1].getBytes()),
                ByteBuffer.wrap(events[2].getBytes()) };
        final int[] crc32 = new int[] { Event.crc32(events[0].getBytes()),
                Event.crc32(events[1].getBytes()),
                Event.crc32(events[2].getBytes()) };
        final UUID channel = UUID.randomUUID();
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        SocketChannel outbound = mock(SocketChannel.class);
        when(handler.getChannel()).thenReturn(outbound);

        SortedMap<BatchIdentity, Batch> pending = new TreeMap<BatchIdentity, Batch>();

        final BatchEventWriter batchWriter = new BatchEventWriter(100, 5,
                                                                  "test");
        assertEquals(BatchEventWriterFSM.Suspended, batchWriter.getState());
        batchWriter.connect(handler);
        assertEquals(BatchEventWriterFSM.Waiting, batchWriter.getState());
        final Node mirror = new Node(0x1638);
        List<Batch> batches = new ArrayList<Batch>();
        for (int i = 0; i < 100; i++) {
            batches.add(new Batch(mirror, channel, i, Arrays.asList(payloads)));
        }
        final AtomicInteger count = new AtomicInteger(-1);
        Answer<Integer> readBatchHeader = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer[] buffers = (ByteBuffer[]) invocation.getArguments()[0];
                int offset = (Integer) invocation.getArguments()[1];
                while (!buffers[offset].hasRemaining()) {
                    offset++;
                }
                BatchHeader header = BatchHeader.readFrom(buffers[offset]);
                buffers[offset].position(buffers[offset].limit());
                assertEquals(channel, header.getChannel());
                assertEquals(count.incrementAndGet(),
                             header.getSequenceNumber());
                assertEquals(mirror, header.getProducerMirror());
                assertEquals(BatchHeader.MAGIC, header.getMagic());
                assertEquals(0, buffers[offset].remaining());
                return BatchHeader.HEADER_BYTE_SIZE;
            }
        };

        Answer<Integer> readBatch = new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer[] buffers = (ByteBuffer[]) invocation.getArguments()[0];
                int offset = (Integer) invocation.getArguments()[1];
                while (!buffers[offset].hasRemaining()) {
                    offset++;
                }
                assert buffers[offset] != null;
                int totalLength = buffers[offset].remaining();
                for (int i = 0; i < 3; i++) {
                    Event event = Event.readFrom(buffers[offset]);
                    byte[] buf = new byte[events[i].getBytes().length];
                    event.getPayload().get(buf);
                    String payload = new String(buf);
                    assertEquals(events[i].getBytes().length, event.size());
                    assertEquals(crc32[i], event.getCrc32());
                    assertTrue("event failed CRC check", event.validate());
                    assertEquals(String.format("unexpected event content '%s'",
                                               payload), events[i], payload);
                }
                assertEquals(0, buffers[offset].remaining());
                return totalLength;
            }
        };
        ByteBuffer[] temp = new ByteBuffer[0];
        OngoingStubbing<Long> ongoingStub = when(outbound.write(any(temp.getClass()),
                                                                any(Integer.class),
                                                                any(Integer.class)));
        for (int i = 0; i < batches.size(); i++) {
            ongoingStub = ongoingStub.thenReturn(0L).thenReturn(0L).thenAnswer(readBatchHeader).thenAnswer(readBatch);
        }

        for (Batch batch : batches) {
            batchWriter.push(batch, pending);
        }
        while (batchWriter.getState() == BatchEventWriterFSM.WriteBatch) {
            batchWriter.writeReady();
        }
        while (batchWriter.getState() != BatchEventWriterFSM.Waiting) {
            batchWriter.writeReady();
        }
        verify(outbound, new Times(400)).write(any(temp.getClass()),
                                               any(Integer.class),
                                               any(Integer.class));
    }
}
