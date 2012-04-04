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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.api.producer.UnknownChannelException;
import com.salesforce.ouroboros.producer.Producer;

public class Source implements EventSource {
    private static final Logger                log                   = LoggerFactory.getLogger(Source.class.getCanonicalName());
    public final ConcurrentHashMap<UUID, Long> channels              = new ConcurrentHashMap<UUID, Long>();
    private Producer                           producer;
    public final Set<UUID>                     failedChannels        = new ConcurrentSkipListSet<UUID>();
    private final Set<UUID>                    pausedChannels        = new ConcurrentSkipListSet<UUID>();
    public final Set<UUID>                     relinquishedPrimaries = new ConcurrentSkipListSet<UUID>();
    private AtomicBoolean                      shutdown              = new AtomicBoolean();

    @Override
    public void assumePrimary(Map<UUID, Long> newPrimaries) {
        System.out.println(String.format("Assuming primary for %s on %s",
                                         newPrimaries, producer.getId()));
        channels.putAll(newPrimaries);
        for (UUID channel : newPrimaries.keySet()) {
            failedChannels.remove(channel);
        }
    }

    @Override
    public void closed(UUID channel) {
        channels.remove(channel);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#deadChannels(java.util.List)
     */
    @Override
    public void deactivated(Collection<UUID> deadChannels) {
        for (UUID channel : deadChannels) {
            channels.remove(channel);
        }
    }

    public Node getId() {
        return producer.getId();
    }

    @Override
    public void opened(UUID channel) {
        channels.put(channel, 0L);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#pauseChannels(java.util.Collection)
     */
    @Override
    public void pause(Collection<UUID> channels) {
        pausedChannels.addAll(channels);
    }

    public void publish(final int batchSize, Executor executor,
                        final CountDownLatch latch, final long targetTimestamp) {
        shutdown.set(false);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                publish(batchSize, latch, targetTimestamp);
                System.out.println(String.format("Stopping publishing on %s",
                                                 producer.getId()));
                shutdown.set(true);
            }
        });
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#relinquishPrimary(java.util.UUID)
     */
    @Override
    public void relinquishPrimary(Collection<UUID> channels) {
        System.out.println(String.format("Relinquishing primary for %s on %s",
                                         channels, producer.getId()));
        relinquishedPrimaries.addAll(channels);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#resume(java.util.Collection)
     */
    @Override
    public void resume(Collection<UUID> channels) {
        pausedChannels.removeAll(channels);
    }

    /**
     * @param producer
     */
    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public void shutdown() {
        shutdown.set(true);
    }

    private void publish(int batchSize, CountDownLatch latch, long target) {
        boolean run = true;
        Long targetTimestamp = Long.valueOf(target);
        while (run && !shutdown.get()) {
            run = false;
            for (Entry<UUID, Long> entry : channels.entrySet()) {
                UUID channel = entry.getKey();
                Long sequenceNumber = entry.getValue();
                if (!sequenceNumber.equals(targetTimestamp)) {
                    run |= true;
                    boolean published = false;
                    ArrayList<ByteBuffer> events = new ArrayList<ByteBuffer>();
                    for (int i = 0; i < batchSize; i++) {
                        events.add(ByteBuffer.wrap(String.format("%s Give me Slack or give me Food or Kill me %s",
                                                                 channel,
                                                                 channel).getBytes()));
                    }
                    int i = 0;
                    long nextTimestamp = sequenceNumber + 1;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e2) {
                        return;
                    }
                    boolean failed = false;
                    while (!published && channels.containsKey(channel)
                           && !shutdown.get()
                           && !pausedChannels.contains(channel)) {
                        try {
                            try {
                                producer.publish(channel, nextTimestamp, events);
                            } catch (UnknownChannelException e) {
                                failed = true;
                                channels.remove(channel);
                                failedChannels.add(channel);
                                continue;
                            } catch (InterruptedException e) {
                                return;
                            }
                            published = true;
                        } catch (RateLimiteExceededException e) {
                            log.info(String.format("Rate limit exceeded for %s on %s",
                                                   channel, producer.getId()));
                            if (i > 20) {
                                log.info(String.format("Giving up on sending event to %s on %s",
                                                       channel,
                                                       producer.getId()));
                                return;
                            }
                            try {
                                Thread.sleep(100 * i++);
                            } catch (InterruptedException e1) {
                                return;
                            }
                        }
                    }
                    if (!failed) {
                        entry.setValue(nextTimestamp);
                    }
                }
            }
        }
        failedChannels.removeAll(relinquishedPrimaries);
        latch.countDown();
    }
}