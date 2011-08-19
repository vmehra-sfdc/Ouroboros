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

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;
import com.lmax.disruptor.ProducerBarrier;
import com.lmax.disruptor.RingBuffer;

/**
 * The Weaver represents the channel buffer process that provides persistent,
 * replicated buffers of events, their subscriptions and the services for
 * publishing and consuming these events.
 * 
 * @author hhildebrand
 * 
 */
public class Weaver implements Bundle {

    private class ConsumerWrapper implements Consumer {
        private Replicator replicator;

        @Override
        public long getSequence() {
            return replicator.getSequence();
        }

        @Override
        public void halt() {
            replicator.halt();
        }

        @Override
        public void run() {
            replicator.run();
        }

        void setReplicator(Replicator replicator) {
            this.replicator = replicator;
        }

    }

    private class ReplicatorFactory implements CommunicationsHandlerFactory {
        final Replicator         replicator;
        private volatile boolean returned = false;

        public ReplicatorFactory(ConsumerBarrier<EventEntry> replicationBarrier) {
            replicator = new Replicator(Weaver.this, replicationBarrier);
        }

        @Override
        public CommunicationsHandler createCommunicationsHandler() {
            if (returned) {
                throw new IllegalStateException("There can only be one");
            }
            returned = true;
            return replicator;
        }

    }

    private class SpindleFactory implements CommunicationsHandlerFactory {
        final ProducerBarrier<EventEntry> replicationBarrier;

        public SpindleFactory(ProducerBarrier<EventEntry> replicationBarrier) {
            this.replicationBarrier = replicationBarrier;
        }

        @Override
        public CommunicationsHandler createCommunicationsHandler() {
            return new Spinner(Weaver.this, replicationBarrier);
        }

    }

    private final long                              maxSegmentSize;
    private final ConcurrentMap<UUID, EventChannel> openChannels     = new ConcurrentHashMap<UUID, EventChannel>();
    private final RingBuffer<EventEntry>            replicationQueue = null;
    private final ServerSocketChannelHandler        replicators;
    private final File                              root;
    private final ServerSocketChannelHandler        spindles;

    public Weaver(WeaverConfigation configuration) throws IOException {
        root = configuration.getRoot();
        maxSegmentSize = configuration.getMaxSegmentSize();
        ConsumerWrapper wrapper = new ConsumerWrapper();
        ReplicatorFactory replicatorFactory = new ReplicatorFactory(
                                                                    replicationQueue.createConsumerBarrier(wrapper));
        wrapper.setReplicator(replicatorFactory.replicator); // hate this shit.
        ProducerBarrier<EventEntry> replicationBarrier = replicationQueue.createProducerBarrier(replicatorFactory.replicator);
        replicators = new ServerSocketChannelHandler(
                                                     "Weaver Replicator",
                                                     configuration.getReplicationSocketOptions(),
                                                     configuration.getReplicationAddress(),
                                                     Executors.newFixedThreadPool(2),
                                                     replicatorFactory);
        spindles = new ServerSocketChannelHandler(
                                                  "Weaver Spindle",
                                                  configuration.getSpindleSocketOptions(),
                                                  configuration.getSpindleAddress(),
                                                  configuration.getSpindles(),
                                                  new SpindleFactory(
                                                                     replicationBarrier));
    }

    @Override
    public EventChannel eventChannelFor(EventHeader header) {
        UUID channelTag = header.getChannel();
        return openChannels.get(channelTag);
    }

    public void open(UUID channel) {
        openChannels.putIfAbsent(channel, new EventChannel(channel, root,
                                                           maxSegmentSize));
    }

    public void start() {
        spindles.start();
        replicators.start();
    }
}
