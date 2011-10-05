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
package com.salesforce.ouroboros.producer.internal;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.SortedMap;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.util.rate.Controller;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpinner {
    @Captor
    ArgumentCaptor<Integer> captor;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPush() throws Exception {
        SocketChannel channel = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Controller rateController = mock(Controller.class);
        Spinner spinner = new Spinner(rateController, 1);
        spinner.handleConnect(channel, handler);

        @SuppressWarnings("unchecked")
        Batch batch = new Batch(UUID.randomUUID(), 0L, Collections.EMPTY_LIST);
        spinner.push(batch);
        Thread.sleep(10);
        spinner.acknowledge(batch);
        verify(rateController).sample(captor.capture());
        assertTrue(10 <= captor.getValue().intValue());
    }

    @Test
    public void testPending() throws Exception {
        SocketChannel outbound = mock(SocketChannel.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Controller rateController = mock(Controller.class);
        Spinner spinner = new Spinner(rateController, 1);
        spinner.handleConnect(outbound, handler);

        long timestamp = 100000L;
        UUID channel = UUID.randomUUID();
        @SuppressWarnings("unchecked")
        Batch batch1 = new Batch(channel, timestamp, Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch2 = new Batch(channel, timestamp + 20,
                                 Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch3 = new Batch(channel, timestamp + 100,
                                 Collections.EMPTY_LIST);
        @SuppressWarnings("unchecked")
        Batch batch4 = new Batch(channel, timestamp + 100,
                                 Collections.EMPTY_LIST);
        assertTrue(spinner.push(batch1));
        assertTrue(spinner.push(batch2));
        assertTrue(spinner.push(batch3));
        spinner.closing(outbound);
        assertFalse(spinner.push(batch4));

        SortedMap<BatchIdentity, Batch> pending = spinner.getPending(channel);
        assertNotNull(pending);
        assertEquals(3, pending.size());
        assertEquals(batch1, pending.get(batch1));
        assertEquals(batch2, pending.get(batch2));
        assertEquals(batch3, pending.get(batch3));
        assertEquals(batch1, pending.firstKey());
        assertEquals(batch3, pending.lastKey());
    }
}
