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
package com.salesforce.ouroboros.producer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.salesforce.ouroboros.util.rate.Controller;
import com.salesforce.ouroboros.util.rate.controllers.RateController;
import com.salesforce.ouroboros.util.rate.controllers.RateLimiter;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Producer {
    private final Controller         controller;
    private final Map<UUID, Spinner> primaryChannels = new ConcurrentHashMap<UUID, Spinner>();

    public Producer(ProducerConfiguration configuration) {
        controller = new RateController(
                                        new RateLimiter(
                                                        configuration.getTargetEventRate(),
                                                        configuration.getTokenLimit(),
                                                        configuration.getMinimumTokenRegenerationTime()),
                                        configuration.getMinimumEventRate(),
                                        configuration.getMaximumEventRate(),
                                        configuration.getSampleWindowSize());
    }

    /**
     * Publish the events on the indicated channel
     * 
     * @param channel
     *            - the id of the event channel
     * @param timestamp
     *            - the timestamp for this batch
     * @param events
     *            - the batch of events to publish to the channel
     * 
     * @throws RateLimiteExceededException
     *             - if the event publishing rate has been exceeded
     * @throws UnknownChannelException
     *             - if the supplied channel id does not exist
     */
    public void publish(UUID channel, long timestamp,
                        Collection<ByteBuffer> events)
                                                      throws RateLimiteExceededException,
                                                      UnknownChannelException {
        Spinner spinner = primaryChannels.get(channel);
        if (spinner == null) {
            throw new UnknownChannelException(
                                              String.format("The channel %s does not exist",
                                                            channel));
        }
        if (!controller.accept(events.size())) {
            throw new RateLimiteExceededException(
                                                  String.format("The rate limit for this producer has been exceeded"));
        }
        spinner.push(new Batch(channel, timestamp, events));
    }
}
