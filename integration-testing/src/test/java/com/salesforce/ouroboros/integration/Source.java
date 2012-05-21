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
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.api.producer.UnknownChannelException;
import com.salesforce.ouroboros.producer.Producer;

public class Source implements EventSource {
    private static final Logger                    log                   = LoggerFactory.getLogger(Source.class.getCanonicalName());
    public final ConcurrentHashMap<UUID, Long>     channels              = new ConcurrentHashMap<UUID, Long>();
    private Producer                               producer;
    public final Set<UUID>                         failedChannels        = new ConcurrentSkipListSet<UUID>();
    private final Set<UUID>                        pausedChannels        = new ConcurrentSkipListSet<UUID>();
    public final Set<UUID>                         relinquishedPrimaries = new ConcurrentSkipListSet<UUID>();
    private final AtomicBoolean                    shutdown              = new AtomicBoolean();
    private final ExecutorService                  executor;
    private final ConcurrentHashMap<UUID, Integer> retries               = new ConcurrentHashMap<UUID, Integer>();
    private volatile CountDownLatch                latch;
    private final AtomicBoolean                    rebalanced            = new AtomicBoolean();

    /**
     * @param executor
     */
    public Source(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public void assumePrimary(Map<UUID, Long> newPrimaries) {
        log.info(String.format("Assuming primary for %s on %s, shutdown: %s",
                               newPrimaries, producer.getId(), shutdown.get()));
        channels.putAll(newPrimaries);
        for (UUID channel : newPrimaries.keySet()) {
            failedChannels.remove(channel);
            retries.put(channel, 0);
        }
    }

    @Override
    public void closed(UUID channel) {
        channels.remove(channel);
        retries.remove(channel);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#deadChannels(java.util.List)
     */
    @Override
    public void deactivated(Collection<UUID> deadChannels) {
        for (UUID channel : deadChannels) {
            channels.remove(channel);
            retries.remove(channel);
        }
    }

    public Node getId() {
        return producer.getId();
    }

    @Override
    public void opened(UUID channel) {
        channels.put(channel, 0L);
        retries.put(channel, 0);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#pauseChannels(java.util.Collection)
     */
    @Override
    public void pause(Collection<UUID> channels) {
        pausedChannels.addAll(channels);
    }

    public void publish(final int batchSize, final CountDownLatch latch,
                        final long targetTimestamp) {
        for (Entry<UUID, Integer> entry : retries.entrySet()) {
            entry.setValue(0);
        }
        this.latch = latch;
        shutdown.set(false);
        log.info(String.format("Starting publishing on %s", producer.getId()));
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    _publish(batchSize, targetTimestamp);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#relinquishPrimary(java.util.UUID)
     */
    @Override
    public void relinquishPrimary(Collection<UUID> channels) {
        log.info(String.format("Relinquishing primary for %s on %s", channels,
                               producer.getId()));
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

    public boolean isShutdown() {
        return shutdown.get();
    }

    @PreDestroy
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            log.info(String.format("Stopping publishing on %s",
                                   producer.getId()));
            failedChannels.removeAll(relinquishedPrimaries);
            latch.countDown();
        }
    }

    private void _publish(final int batchSize, final Long targetTimestamp) {
        Deque<Runnable> tasks = new LinkedList<Runnable>();

        for (Entry<UUID, Long> entry : channels.entrySet()) {
            if (!entry.getValue().equals(targetTimestamp)) {
                tasks.add(publishBatch(tasks, batchSize, entry, targetTimestamp));
            }
        }
        if (shutdown.get()) {
            log.info(String.format("Aborting publishing on %s as we are already shutdown",
                                   producer.getId()));
            return;
        }
        if (tasks.size() == 0) {
            if (rebalanced.get()) {
                shutdown();
                return;
            }
            log.info(String.format("currently no event to publish on %s ",
                                   producer.getId()));
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        return;
                    }
                    try {
                        _publish(batchSize, targetTimestamp);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });
            return;
        }
        tasks.add(new Runnable() {
            @Override
            public void run() {
                try {
                    _publish(batchSize, targetTimestamp);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        executor.execute(tasks.removeFirst());
    }

    private Runnable publishBatch(final Deque<Runnable> tasks, int batchSize,
                                  final Entry<UUID, Long> entry,
                                  final Long targetTimestamp) {
        final ArrayList<ByteBuffer> events = new ArrayList<ByteBuffer>();
        for (int i = 0; i < batchSize; i++) {
            events.add(ByteBuffer.wrap(String.format("%s Give me Slack or give me Food or Kill me %s",
                                                     entry.getKey(),
                                                     entry.getKey()).getBytes()));
        }
        return new Runnable() {
            public void run() {
                try {
                    publishBatch(tasks, entry, events, targetTimestamp);
                } catch (Throwable e) {
                    e.printStackTrace();
                }

            }
        };
    }

    private void publishBatch(Deque<Runnable> tasks, Entry<UUID, Long> entry,
                              ArrayList<ByteBuffer> events, Long targetTimestamp) {
        if (shutdown.get()) {
            return;
        }
        long nextTimestamp = entry.getValue() + 1;
        UUID channel = entry.getKey();
        if (channels.containsKey(channel) && !pausedChannels.contains(channel)
            && !entry.getValue().equals(targetTimestamp)) {
            Integer retryCount = retries.get(channel);
            try {
                try {
                    producer.publish(channel, nextTimestamp, events);
                    entry.setValue(nextTimestamp);
                    retries.put(channel, 0);
                } catch (UnknownChannelException e) {
                    channels.remove(channel);
                    failedChannels.add(channel);
                } catch (InterruptedException e) {
                    return;
                }
            } catch (RateLimiteExceededException e) {
                log.info(String.format("Rate limit exceeded for %s on %s",
                                       channel, producer.getId()));
                if (retryCount > 20) {
                    log.info(String.format("Giving up on sending event to %s on %s",
                                           channel, producer.getId()));
                    shutdown();
                } else {
                    retries.put(channel, retryCount + 1);
                    try {
                        Thread.sleep((retryCount + 1) * 100);
                    } catch (InterruptedException e1) {
                        return;
                    }
                }
            }
        } else {
            log.info(String.format("skipping publishing for channel %s, seq no %s on %s",
                                   entry.getKey(), entry.getValue(),
                                   producer.getId()));
        }
        if (!tasks.isEmpty()) {
            executor.execute(tasks.removeFirst());
        }
    }

    public void rebalanced() {
        rebalanced.set(true);
    }
}