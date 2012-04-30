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
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.source.Acknowledger;
import com.salesforce.ouroboros.spindle.source.Appender;

/**
 * The entry representing the appending of an event.
 * 
 * @author hhildebrand
 * 
 */
public class EventEntry {
    private final Appender              appender;
    private Acknowledger                acknowledger;
    private EventChannel                eventChannel;
    private SocketChannelHandler        handler;
    private final ReplicatedBatchHeader header = new ReplicatedBatchHeader();
    private EventEntry                  next;
    private Segment                     segment;

    public EventEntry(Appender appender) {
        this.appender = appender;
    }

    public EventEntry delink() {
        EventEntry current = next;
        next = null;
        return current;
    }

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

    public void free() {
        appender.free(this);
        handler.selectForRead();
    }

    public EventEntry link(EventEntry h) {
        next = h;
        return this;
    }

    public void set(BatchHeader batchHeader, long offset, int startPosition,
                    EventChannel eventChannel, Segment segment,
                    Acknowledger acknowledger, SocketChannelHandler handler) {
        header.set(batchHeader, offset, startPosition);
        this.eventChannel = eventChannel;
        this.segment = segment;
        this.acknowledger = acknowledger;
        this.handler = handler;
    }

}
