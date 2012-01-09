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
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.SortedMap;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.spinner.SpinnerContext.SpinnerFSM;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpinner {
    @Captor
    ArgumentCaptor<Batch> captor;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testHandshake() throws Exception {
        SocketChannel channel = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(channel);
        Producer producer = mock(Producer.class);
        final Node node = new Node(0x1638);
        when(producer.getId()).thenReturn(node);
        Spinner spinner = new Spinner(producer, 100);

        doReturn(0).doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                assertEquals(Spinner.MAGIC, buffer.getInt());
                Node n = new Node(buffer);
                assertEquals(node, n);
                return Spinner.HANDSHAKE_BYTE_SIZE;
            }
        }).when(channel).write(isA(ByteBuffer.class));

        assertEquals(SpinnerFSM.Suspended, spinner.getState());
        spinner.connect(handler);
        assertEquals(SpinnerFSM.Handshake, spinner.getState());
        spinner.writeReady();
        assertEquals(SpinnerFSM.Established, spinner.getState());
        verify(channel, new Times(2)).write(isA(ByteBuffer.class));
    }

    @Test
    public void testPending() throws Exception {
        SocketChannel outbound = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(outbound);
        Producer producer = mock(Producer.class);
        Node node = new Node(0x1638);
        when(producer.getId()).thenReturn(node);
        Spinner spinner = new Spinner(producer, 100);
        spinner.connect(handler);

        long timestamp = 100000L;
        UUID channel = UUID.randomUUID();
        Node mirror = new Node(0x1638);
        @SuppressWarnings("unchecked")
        Batch batch1 = new Batch(mirror, channel, timestamp,
                                 Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch2 = new Batch(mirror, channel, timestamp + 20,
                                 Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch3 = new Batch(mirror, channel, timestamp + 100,
                                 Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch4 = new Batch(mirror, channel, timestamp + 140,
                                 Collections.EMPTY_LIST);
        spinner.push(batch1);
        spinner.push(batch2);
        spinner.push(batch3);
        spinner.closing();
        spinner.push(batch4);

        SortedMap<BatchIdentity, Batch> pending = spinner.getPending(channel);
        assertNotNull(pending);
        assertEquals(3, pending.size());
        assertEquals(batch1, pending.get(batch1));
        assertEquals(batch2, pending.get(batch2));
        assertEquals(batch3, pending.get(batch3));
        assertEquals(batch1, pending.firstKey());
        assertEquals(batch3, pending.lastKey());
    }

    @Test
    public void testPush() throws Exception {
        SocketChannel channel = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        when(handler.getChannel()).thenReturn(channel);
        Producer producer = mock(Producer.class);
        Node node = new Node(0x1638);
        when(producer.getId()).thenReturn(node);
        Spinner spinner = new Spinner(producer, 100);
        spinner.connect(handler);

        Node mirror = new Node(0x1638);
        @SuppressWarnings("unchecked")
        Batch batch = new Batch(mirror, UUID.randomUUID(), 0L,
                                Collections.EMPTY_LIST);
        spinner.push(batch);
        Thread.sleep(10);
        spinner.acknowledge(batch);
        verify(producer).acknowledge(captor.capture());
        assertTrue(10 <= captor.getValue().interval());
    }
}
