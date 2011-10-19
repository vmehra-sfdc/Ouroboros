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

import java.util.UUID;

import com.salesforce.ouroboros.Node;

/**
 * The interface which provides the segments for appending within a channel,
 * based on the new event's header.
 * 
 * @author hhildebrand
 * 
 */
public interface Bundle {

    /**
     * @return the Node id of the bundle
     */
    Node getId();

    /**
     * Answer the event channel the event is part of.
     * 
     * @param channelId
     *            - the id of the channel
     * @return the EventChannel for this event, or null if no such channel
     *         exists.
     */
    public abstract EventChannel eventChannelFor(UUID channelId);

    /**
     * Map the producer node to the acknowledger
     * 
     * @param producer
     * @param acknowledger
     */
    void map(Node producer, Acknowledger acknowledger);

    /**
     * Answer the Acknowledger associated the node
     * 
     * @param node
     * @return the Acknowledger associated with the node
     */
    Acknowledger getAcknowledger(Node node);
}
