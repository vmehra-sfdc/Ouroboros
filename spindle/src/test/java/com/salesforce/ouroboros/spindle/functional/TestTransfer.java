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
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.spindle.Segment;
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
                                              Executors.newCachedThreadPool(),
                                              new CommunicationsHandlerFactory() {
                                                  @Override
                                                  public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
                                                      return handler;
                                                  }
                                              });
    }

    @After
    public void teardown() throws IOException {
        Node dummy = new Node(0);
        if (spindleHandler != null) {
            spindleHandler.terminate();
        }
        if (replicatorHandler != null) {
            replicatorHandler.terminate();
        }
        if (primaryEventChannel != null) {
            primaryEventChannel.close(dummy);
        }
        if (primarySinkHandler != null) {
            primarySinkHandler.terminate();
        }
        if (mirrorSinkHandler != null) {
            mirrorSinkHandler.terminate();
        }

        if (primaryEventChannel != null) {
            primaryEventChannel.close(dummy);
        }
        if (mirrorEventChannel != null) {
            mirrorEventChannel.close(dummy);
        }

        if (primarySinkEventChannel != null) {
            primarySinkEventChannel.close(dummy);
        }
        if (mirrorSinkEventChannel != null) {
            mirrorSinkEventChannel.close(dummy);
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

        @SuppressWarnings("unchecked")
        ConcurrentMap<File, Segment> segmentCache = mock(ConcurrentMap.class);
        when(primaryBundle.getId()).thenReturn(primaryNode);
        when(mirrorBundle.getId()).thenReturn(mirrorNode);

        Spindle spindle = new Spindle(primaryBundle);
        Replicator primaryReplicator = new Replicator(primaryBundle,
                                                      mirrorNode, rendezvous);
        Replicator mirrorReplicator = new Replicator(mirrorBundle);
        primaryEventChannel = new EventChannel(Role.PRIMARY, mirrorNode,
                                               channelId, primaryRoot,
                                               maxSegmentSize,
                                               primaryReplicator, segmentCache);

        mirrorEventChannel = new EventChannel(Role.MIRROR, primaryNode,
                                              channelId, mirrorRoot,
                                              maxSegmentSize, null,
                                              segmentCache);

        when(primaryBundle.eventChannelFor(channelId)).thenReturn(primaryEventChannel);
        when(mirrorBundle.eventChannelFor(channelId)).thenReturn(mirrorEventChannel);

        startSpindleAndReplicator(spindle, mirrorReplicator);

        spindleHandler.connectTo(replicatorHandler.getLocalAddress(),
                                 primaryReplicator);

        int batches = 2500;
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
        Producer producer = new Producer(channelId, producerLatch, batches,
                                         batchSize, producerNode, primaryNode);
        spindleHandler.connectTo(spindleHandler.getLocalAddress(), producer);
        assertTrue("Did not publish all events in given time",
                   producerLatch.await(120, TimeUnit.SECONDS));
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

        primarySinkEventChannel = new EventChannel(Role.PRIMARY,
                                                   mirrorSinkNode, channelId,
                                                   primarySinkRoot,
                                                   maxSegmentSize,
                                                   primaryReplicator,
                                                   segmentCache);

        mirrorSinkEventChannel = new EventChannel(Role.MIRROR, primarySinkNode,
                                                  channelId, mirrorSinkRoot,
                                                  maxSegmentSize, null,
                                                  segmentCache);

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
}
