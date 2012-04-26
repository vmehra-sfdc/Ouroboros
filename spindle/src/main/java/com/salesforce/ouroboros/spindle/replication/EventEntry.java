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
package com.salesforce.ouroboros.spindle.replication;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.source.Acknowledger;

/**
 * The entry representing the appending of an event.
 * 
 * @author hhildebrand
 * 
 */
public class EventEntry {
    private Acknowledger          acknowledger;
    private EventChannel          eventChannel;
    private SocketChannelHandler  handler;
    private ReplicatedBatchHeader header;
    private Segment               segment;

    /**
     * @return the acknowledger
     */
    public Acknowledger getAcknowledger() {
        return acknowledger;
    }

    /**
     * @return the eventChannel
     */
    public EventChannel getEventChannel() {
        return eventChannel;
    }

    /**
     * @return the handler
     */
    public SocketChannelHandler getHandler() {
        return handler;
    }

    /**
     * @return the header
     */
    public ReplicatedBatchHeader getHeader() {
        return header;
    }

    /**
     * @return the segment
     */
    public Segment getSegment() {
        return segment;
    }

    /**
     * @param acknowledger
     *            the acknowledger to set
     */
    public void setAcknowledger(Acknowledger acknowledger) {
        this.acknowledger = acknowledger;
    }

    public void setEntry(ReplicatedBatchHeader header,
                         EventChannel eventChannel, Segment segment,
                         Acknowledger acknowledger, SocketChannelHandler handler) {
        setHeader(header);
        setEventChannel(eventChannel);
        setSegment(segment);
        setAcknowledger(acknowledger);
        setHandler(handler);
    }

    /**
     * @param eventChannel
     *            the eventChannel to set
     */
    public void setEventChannel(EventChannel eventChannel) {
        this.eventChannel = eventChannel;
    }

    /**
     * @param handler
     *            the handler to set
     */
    public void setHandler(SocketChannelHandler handler) {
        this.handler = handler;
    }

    /**
     * @param header
     *            the header to set
     */
    public void setHeader(ReplicatedBatchHeader header) {
        this.header = header;
    }

    /**
     * @param segment
     *            the segment to set
     */
    public void setSegment(Segment segment) {
        this.segment = segment;
    }

}
