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
package com.salesforce.ouroboros.spindle.functional;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.source.Acknowledger;
import com.salesforce.ouroboros.spindle.source.Spindle;
import com.salesforce.ouroboros.spindle.transfer.Sink;
import com.salesforce.ouroboros.spindle.transfer.Xerox;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestTransfer {

    private EventChannel               mirrorEventChannel;
    private File                       mirrorRoot;
    private ServerSocketChannelHandler mirrorSinkHandler;
    private File                       mirrorSinkRoot;
    private EventChannel               primaryEventChannel;
    private File                       primaryRoot;
    private ServerSocketChannelHandler primarySinkHandler;
    private File                       primarySinkRoot;
    private ServerSocketChannelHandler replicatorHandler;
    private ServerSocketChannelHandler spindleHandler;
    private EventChannel               primarySinkEventChannel;
    private EventChannel               mirrorSinkEventChannel;
    UUID                               channelId      = UUID.randomUUID();
    int                                maxSegmentSize = 1024 * 1024;

    public ServerSocketChannelHandler createHandler(String label,
                                                    final CommunicationsHandler handler,
                                                    SocketOptions socketOptions)
                                                                                throws IOException {
        return new ServerSocketChannelHandler(
                                              label,
                                              socketOptions,
                                              new InetSocketAddress(
                                                                    "127.0.0.1",
                                                                    0),
                                              Executors.newFixedThreadPool(5),
                                              new CommunicationsHandlerFactory() {

                                                  @Override
                                                  public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
                                                      return handler;
                                                  }
                                              });
    }

    @After
    public void teardown() throws IOException {
        if (spindleHandler != null) {
            spindleHandler.terminate();
        }
        if (replicatorHandler != null) {
            replicatorHandler.terminate();
        }
        if (primaryEventChannel != null) {
            primaryEventChannel.close();
        }
        if (primarySinkHandler != null) {
            primarySinkHandler.terminate();
        }
        if (mirrorSinkHandler != null) {
            mirrorSinkHandler.terminate();
        }

        if (primaryEventChannel != null) {
            primaryEventChannel.close();
        }
        if (mirrorEventChannel != null) {
            mirrorEventChannel.close();
        }

        if (primarySinkEventChannel != null) {
            primarySinkEventChannel.close();
        }
        if (mirrorSinkEventChannel != null) {
            mirrorSinkEventChannel.close();
        }

        if (primaryRoot != null) {
            Utils.deleteDirectory(primaryRoot);
        }
        if (mirrorRoot != null) {
            Utils.deleteDirectory(mirrorRoot);
        }
        if (primarySinkRoot != null) {
            Utils.deleteDirectory(primarySinkRoot);
        }
        if (mirrorSinkRoot != null) {
            Utils.deleteDirectory(mirrorSinkRoot);
        }
    }

    @Test
    public void testReplicationTransfer() throws Exception {
        setupTemporaryRoots();

        Node producerNode = new Node(0);
        Node primaryNode = new Node(1);
        Node mirrorNode = new Node(2);
        Rendezvous rendezvous = mock(Rendezvous.class);
        final Bundle primaryBundle = mock(Bundle.class);
        final Bundle mirrorBundle = mock(Bundle.class);
        when(primaryBundle.getId()).thenReturn(primaryNode);
        when(mirrorBundle.getId()).thenReturn(mirrorNode);

        Spindle spindle = new Spindle(primaryBundle);
        Replicator primaryReplicator = new Replicator(primaryBundle,
                                                      mirrorNode, rendezvous);
        Replicator mirrorReplicator = new Replicator(mirrorBundle);
        primaryEventChannel = new EventChannel(Role.PRIMARY, channelId,
                                               primaryRoot, maxSegmentSize,
                                               primaryReplicator);

        mirrorEventChannel = new EventChannel(Role.MIRROR, channelId,
                                              mirrorRoot, maxSegmentSize, null);

        when(primaryBundle.eventChannelFor(channelId)).thenReturn(primaryEventChannel);
        when(mirrorBundle.eventChannelFor(channelId)).thenReturn(mirrorEventChannel);

        startSpindleAndReplicator(spindle, mirrorReplicator);

        spindleHandler.connectTo(replicatorHandler.getLocalAddress(),
                                 primaryReplicator);

        int batches = 3000;
        int batchSize = 100;
        final CountDownLatch ackLatch = new CountDownLatch(batches);
        Acknowledger ack = mock(Acknowledger.class);
        Answer<Void> acknowledge = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ackLatch.countDown();
                return null;
            }
        };
        doAnswer(acknowledge).when(ack).acknowledge(isA(UUID.class),
                                                    isA(Long.class));

        when(primaryBundle.getAcknowledger(isA(Node.class))).thenReturn(ack);
        when(mirrorBundle.getAcknowledger(isA(Node.class))).thenReturn(ack);

        CountDownLatch producerLatch = new CountDownLatch(1);
        Producer producer = new Producer(producerLatch, batches, batchSize,
                                         producerNode);
        spindleHandler.connectTo(spindleHandler.getLocalAddress(), producer);
        assertTrue("Did not publish all events in given time",
                   producerLatch.await(60, TimeUnit.SECONDS));
        System.out.println("Events published");
        assertTrue("Did not receive acknowledgement from all event writes and replications",
                   ackLatch.await(60, TimeUnit.SECONDS));

        // set up the xeroxes and sinks for both the primary and secondary
        Node primarySinkNode = new Node(3);
        Node mirrorSinkNode = new Node(4);
        Bundle primarySinkBundle = mock(Bundle.class);
        Bundle mirrorSinkBundle = mock(Bundle.class);
        when(primarySinkBundle.getId()).thenReturn(primarySinkNode);
        when(mirrorSinkBundle.getId()).thenReturn(mirrorSinkNode);

        Sink primarySink = new Sink(primarySinkBundle);
        Sink mirrorSink = new Sink(mirrorSinkBundle);

        primarySinkEventChannel = new EventChannel(Role.PRIMARY, channelId,
                                                   primarySinkRoot,
                                                   maxSegmentSize,
                                                   primaryReplicator);

        mirrorSinkEventChannel = new EventChannel(Role.MIRROR, channelId,
                                                  mirrorSinkRoot,
                                                  maxSegmentSize, null);

        when(primarySinkBundle.xeroxEventChannel(channelId)).thenReturn(primarySinkEventChannel);
        when(mirrorSinkBundle.xeroxEventChannel(channelId)).thenReturn(mirrorSinkEventChannel);

        final CountDownLatch xeroxLatch = new CountDownLatch(2);
        Rendezvous xeroxRendezvous = mock(Rendezvous.class);
        Answer<Void> meet = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                xeroxLatch.countDown();
                return null;
            }
        };
        doAnswer(meet).when(xeroxRendezvous).meet();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                fail("Rendezvous cancelled during xerox");
                return null;
            }
        }).when(xeroxRendezvous).cancel();

        Xerox primaryXerox = new Xerox(primaryNode, primarySinkNode);
        primaryXerox.setRendezvous(xeroxRendezvous);
        Xerox mirrorXerox = new Xerox(mirrorNode, mirrorSinkNode);
        mirrorXerox.setRendezvous(xeroxRendezvous);

        primaryXerox.addChannel(primaryEventChannel);
        mirrorXerox.addChannel(mirrorEventChannel);

        startSinks(primarySink, mirrorSink);

        spindleHandler.connectTo(primarySinkHandler.getLocalAddress(),
                                 primaryXerox);

        spindleHandler.connectTo(mirrorSinkHandler.getLocalAddress(),
                                 mirrorXerox);

        assertTrue("Xerox never completed",
                   xeroxLatch.await(60, TimeUnit.SECONDS));

    }

    private void setupTemporaryRoots() throws IOException {
        primaryRoot = File.createTempFile("primary", ".channel");
        primaryRoot.delete();
        primaryRoot.mkdirs();
        primaryRoot.deleteOnExit();
        mirrorRoot = File.createTempFile("mirror", ".channel");
        mirrorRoot.delete();
        mirrorRoot.mkdirs();
        mirrorRoot.deleteOnExit();

        primarySinkRoot = File.createTempFile("primary", ".channel");
        primarySinkRoot.delete();
        primarySinkRoot.mkdirs();
        primarySinkRoot.deleteOnExit();
        mirrorSinkRoot = File.createTempFile("mirror", ".channel");
        mirrorSinkRoot.delete();
        mirrorSinkRoot.mkdirs();
        mirrorSinkRoot.deleteOnExit();
    }

    private void startSinks(final Sink primarySink, final Sink mirrorSink)
                                                                          throws IOException {
        SocketOptions socketOptions = new SocketOptions();

        primarySinkHandler = createHandler("primary Sink handler", primarySink,
                                           socketOptions);
        mirrorSinkHandler = createHandler("mirror Sink handler", mirrorSink,
                                          socketOptions);
        primarySinkHandler.start();
        mirrorSinkHandler.start();
    }

    private void startSpindleAndReplicator(final Spindle spindle,
                                           final Replicator mirrorReplicator)
                                                                             throws IOException {
        SocketOptions socketOptions = new SocketOptions();

        spindleHandler = createHandler("spindle handler", spindle,
                                       socketOptions);
        replicatorHandler = createHandler("replicator handler",
                                          mirrorReplicator, socketOptions);
        spindleHandler.start();
        replicatorHandler.start();
    }

    private class Producer implements CommunicationsHandler {
        private final int            numberOfBatches;
        private SocketChannelHandler handler;
        private final AtomicInteger  batches   = new AtomicInteger();
        private final ByteBuffer     batch;
        private final AtomicInteger  timestamp = new AtomicInteger(0);
        private BatchHeader          currentHeader;
        private final Node           node;
        private final int            batchLength;
        private final CountDownLatch latch;

        private Producer(CountDownLatch latch, int batches, int batchSize,
                         Node node) {
            this.latch = latch;
            this.node = node;
            this.numberOfBatches = batches;
            Event event = event();
            batchLength = batchSize * event.totalSize();
            batch = ByteBuffer.allocate(batchLength);
            for (int i = 0; i < batchSize; i++) {
                event.rewind();
                batch.put(event.getBytes());
            }
        }

        private void nextBatch() {
            int batchNumber = batches.get();
            if (batchNumber == numberOfBatches) {
                System.out.println();
                latch.countDown();
                return;
            }
            if (batchNumber % 100 == 0) {
                System.out.println();
                System.out.print(Integer.toString(batchNumber));
            }
            if (batchNumber % 10 == 0) {
                System.out.print('.');
            }
            batches.incrementAndGet();
            currentHeader = new BatchHeader(node, batchLength,
                                            BatchHeader.MAGIC, channelId,
                                            timestamp.incrementAndGet());
            batch.rewind();
            handler.selectForWrite();
        }

        private Event event() {
            byte[] payload = String.format("%s Give me Slack, or give me Food, or Kill me %s",
                                           channelId, channelId).getBytes();
            ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
            return new Event(666, payloadBuffer);
        }

        @Override
        public void closing() {
        }

        @Override
        public void accept(SocketChannelHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void connect(SocketChannelHandler handler) {
            this.handler = handler;
            ByteBuffer buffer = ByteBuffer.allocate(Spindle.HANDSHAKE_SIZE);
            buffer.putInt(Spindle.MAGIC);
            node.serialize(buffer);
            buffer.flip();
            try {
                while (buffer.hasRemaining()) {
                    handler.getChannel().write(buffer);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            nextBatch();
        }

        @Override
        public void readReady() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeReady() {
            if (currentHeader.hasRemaining()) {
                try {
                    if (currentHeader.write(handler.getChannel()) < 0) {
                        handler.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                if (currentHeader.hasRemaining()) {
                    handler.selectForWrite();
                    return;
                }
            }
            try {
                handler.getChannel().write(batch);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            if (batch.hasRemaining()) {
                handler.selectForWrite();
            } else {
                nextBatch();
            }
        }

    }
}