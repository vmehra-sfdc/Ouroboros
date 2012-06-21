/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.spindle.shuttle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.EventSpan;
import com.salesforce.ouroboros.SubscriptionEvent;
import com.salesforce.ouroboros.batch.BatchWriter;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;

/**
 * @author hhildebrand
 * 
 */
public class PushNotification extends BatchWriter<EventSpan> implements
        SubscriptionHandler {
    private static final Logger log = LoggerFactory.getLogger(PushNotification.class);

    private final Bundle        bundle;

    /**
     * @param ebs
     * @param bs
     * @param nameFormat
     */
    public PushNotification(int ebs, int bs, String nameFormat, Bundle bundle) {
        super(ebs, bs, nameFormat);
        this.bundle = bundle;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.shuttle.SubscriptionHandler#handle(com.salesforce.ouroboros.SubscriptionEvent)
     */
    @Override
    public void handle(SubscriptionEvent event) {
        if (event.subscribe) {
            EventChannel channel = bundle.eventChannelFor(event.channel);
            if (channel != null) {
                channel.subscribe(this);
                if (log.isInfoEnabled()) {
                    log.info(String.format("New subscription for %s on %s",
                                           event.channel, fsm.getName()));
                }
            } else {
                if (log.isInfoEnabled()) {
                    log.info(String.format("New subscription for non existent channel %s on %s",
                                           event.channel, fsm.getName()));
                }
            }
        } else {
            EventChannel channel = bundle.eventChannelFor(event.channel);
            if (channel != null) {
                channel.subscribe(this);
                if (log.isInfoEnabled()) {
                    log.info(String.format("Unsubscribing from %s on %s",
                                           event.channel, fsm.getName()));
                }
            } else {
                if (log.isInfoEnabled()) {
                    log.info(String.format("Unsubscribing from a non existent channel %s on %s",
                                           event.channel, fsm.getName()));
                }
            }
        }
    }

}
