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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.api.producer.UnknownChannelException;
import com.salesforce.ouroboros.producer.Producer;

public class Source implements EventSource {
    private static final Logger                log            = Logger.getLogger(Source.class.getCanonicalName());
    public final ConcurrentHashMap<UUID, Long> channels       = new ConcurrentHashMap<UUID, Long>();
    private Producer                           producer;
    public final ArrayList<UUID>               failedChannels = new ArrayList<UUID>();

    @Override
    public void assumePrimary(Map<UUID, Long> newPrimaries) {
        channels.putAll(newPrimaries);
    }

    @Override
    public void closed(UUID channel) {
        channels.remove(channel);
    }

    @Override
    public void opened(UUID channel) {
        channels.put(channel, 0L);
    }

    /**
     * @param producer
     */
    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public void publish(final int batchCount, final int batchSize,
                        Executor executor, final CountDownLatch latch,
                        final long initialTimestamp) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                publish(batchCount, batchSize, latch, initialTimestamp);
            }
        });
    }

    private void publish(int batchCount, int batchSize, CountDownLatch latch,
                         long initialTimestamp) {
        for (long timestamp = initialTimestamp; timestamp < initialTimestamp
                                                            + batchCount; timestamp++) {
            for (UUID channel : channels.keySet()) {
                ArrayList<ByteBuffer> events = new ArrayList<ByteBuffer>();
                for (int i = 0; i < batchSize; i++) {
                    events.add(ByteBuffer.wrap(String.format("%s Give me Slack or give me Food or Kill me %s",
                                                             channel, channel).getBytes()));
                }
                boolean published = false;
                int i = 0;
                while (!published && channels.containsKey(channel)) {
                    try {
                        try {
                            producer.publish(channel, timestamp, events);
                        } catch (UnknownChannelException e) {
                            failedChannels.add(channel);
                            continue;
                        } catch (InterruptedException e) {
                            return;
                        }
                        published = true;
                    } catch (RateLimiteExceededException e) {
                        log.info(String.format("Rate limit exceeded for %s on %s",
                                               channel, producer.getId()));
                        if (i > 10) {
                            log.info(String.format("Giving up on sending event to %s on %s",
                                                   channel, producer.getId()));
                            return;
                        }
                        try {
                            Thread.sleep(100 * i++);
                        } catch (InterruptedException e1) {
                            return;
                        }
                    }
                }
            }
        }
        latch.countDown();
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
}