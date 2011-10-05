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

import java.nio.channels.SocketChannel;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.util.rate.Controller;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {
    private final BatchAcknowledgement               ack;
    private final Controller                         controller;
    private final Logger                             log         = Logger.getLogger(Spinner.class.getCanonicalName());
    private final NavigableMap<BatchIdentity, Batch> pending;
    private final AtomicInteger                      sampleCount = new AtomicInteger();
    private final int                                sampleFrequency;
    private final BatchWriter                        writer;
    private SocketChannelHandler                     handler;

    public Spinner(Controller rateController, int sampleFrequency) {
        writer = new BatchWriter();
        ack = new BatchAcknowledgement(this);
        pending = new ConcurrentSkipListMap<BatchIdentity, Batch>();
        controller = rateController;
        this.sampleFrequency = sampleFrequency;
    }

    /**
     * Acknowledge a successful commit of an event batch.
     * 
     * @param ack
     *            - the identity of the successfully commited event batch
     */
    public void acknowledge(BatchIdentity ack) {
        Batch batch = pending.remove(ack);
        if (batch != null) {
            if (sampleCount.incrementAndGet() % sampleFrequency == 0) {
                controller.sample(batch.acknowledged());
            }
            if (log.isLoggable(Level.FINEST)) {
                log.finest(String.format("Batch %s acknowledged", ack));
            }
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Acknowledgement for %s, but no batch pending...",
                                       ack));
            }
        }
    }

    /**
     * Close the receiver
     */
    public void close() {
        handler.close();
    }

    @Override
    public void closing(SocketChannel channel) {
        ack.closing(channel);
        writer.closing(channel);
    }

    /**
     * Retrieve all the unacknowledged batch events for the channel.
     * 
     * @param channel
     *            - the Channel
     * @return a SortedMap of all the batch events for the channel that have not
     *         been acknowledged. All pending batches for this channel are
     *         stored in ascending timestamp order
     */
    public SortedMap<BatchIdentity, Batch> getPending(UUID channel) {
        return pending.subMap(new BatchIdentity(channel, 0),
                              new BatchIdentity(channel, Long.MAX_VALUE));
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        this.handler = handler;
        ack.handleConnect(channel, handler);
        writer.handleConnect(channel, handler);
    }

    @Override
    public void handleRead(SocketChannel channel) {
        ack.handleRead(channel);
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        writer.handleWrite(channel);
    }

    /**
     * Push the batch of events.
     * 
     * @param events
     *            - the batch to push
     * @return true, if the push is allowed, false if the system is closed or in
     *         the error state.
     */
    public boolean push(Batch events) {
        return writer.push(events, pending);
    }

    /**
     * Close the multiplexed channel handled by this spinner
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        // TODO Auto-generated method stub

    }
}
