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
package com.salesforce.ouroboros.integration;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.Batch;
import com.salesforce.ouroboros.producer.Coordinator;
import com.salesforce.ouroboros.producer.Spinner;
import com.salesforce.ouroboros.spindle.Acknowledger;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.ReplicatedBatchHeader;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.Spindle;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpinnerSpindle {
    @Test
    public void testSimpleAppend() throws Exception {
        final AtomicBoolean committed = new AtomicBoolean();

        File segmentFile = File.createTempFile("simpleAppend", "segment");
        segmentFile.delete();
        segmentFile.deleteOnExit();
        EventChannel eventChannel = mock(EventChannel.class);
        Segment segment = new Segment(segmentFile);
        final Bundle bundle = mock(Bundle.class);
        Node mirror = new Node(3);
        UUID channel = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        long offset = 0;
        EventChannel.AppendSegment appendSegment = new EventChannel.AppendSegment(
                                                                                  segment,
                                                                                  offset);

        when(bundle.eventChannelFor(channel)).thenReturn(eventChannel);
        when(eventChannel.isDuplicate(isA(BatchHeader.class))).thenReturn(false);
        when(eventChannel.segmentFor(isA(BatchHeader.class))).thenReturn(appendSegment);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                committed.set(true);
                return null;
            }
        }).when(eventChannel).append(isA(ReplicatedBatchHeader.class),
                                     eq(segment), isA(Acknowledger.class));

        final ArrayList<Spindle> spindles = new ArrayList<Spindle>();

        CommunicationsHandlerFactory spindleFactory = new CommunicationsHandlerFactory() {

            @Override
            public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
                Spindle spindle = new Spindle(bundle);
                spindles.add(spindle);
                return spindle;
            }
        };
        Executor spindleExec = Executors.newSingleThreadExecutor();
        SocketOptions spindleSocketOptions = new SocketOptions();
        ServerSocketChannelHandler spindleHandler = new ServerSocketChannelHandler(
                                                                                   "Spindle Handler",
                                                                                   spindleSocketOptions,
                                                                                   new InetSocketAddress(
                                                                                                         "127.0.0.1",
                                                                                                         0),
                                                                                   spindleExec,
                                                                                   spindleFactory);
        spindleHandler.start();
        InetSocketAddress spindleAddress = spindleHandler.getLocalAddress();

        Executor spinnerExec = Executors.newSingleThreadExecutor();
        SocketOptions spinnerSocketOptions = new SocketOptions();
        ChannelHandler spinnerHandler = new ChannelHandler(
                                                           "Spinner Handler",
                                                           spinnerSocketOptions,
                                                           spinnerExec);
        spinnerHandler.start();

        Node coordNode = new Node(2);
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getId()).thenReturn(coordNode);
        Spinner spinner = new Spinner(coordinator);

        spindleHandler.connectTo(spindleAddress, spinner);

        Util.waitFor("No connection", new Util.Condition() {
            @Override
            public boolean value() {
                return !spindles.isEmpty();
            }
        }, 2000, 100);

        assertEquals(1, spindles.size());
        Spindle spindle = spindles.get(0);
        assertEquals(Spindle.State.ESTABLISHED, spindle.getState());

        assertEquals(Spinner.State.ESTABLISHED, spinner.getState());

        String[] eventContent = new String[] { "give me slack",
                "or give me food", "or kill me" };
        ArrayList<ByteBuffer> eventBuffers = new ArrayList<ByteBuffer>();
        for (String event : eventContent) {
            eventBuffers.add(ByteBuffer.wrap(event.getBytes()));
        }

        spinner.push(new Batch(mirror, channel, timestamp, eventBuffers));

        Util.waitFor("Did not commit event batch", new Util.Condition() {

            @Override
            public boolean value() {
                return committed.get();
            }
        }, 2000, 100);

        segment.position(0);

        Event event;
        ByteBuffer payload;
        byte[] bytes;
        for (String content : eventContent) {
            event = new Event(segment);
            assertNotNull(event);
            assertTrue(event.validate());
            payload = event.getPayload();
            bytes = new byte[payload.remaining()];
            payload.get(bytes);
            assertEquals(content, new String(bytes));
        }

        spindleHandler.terminate();
        spinnerHandler.terminate();
    }
}
