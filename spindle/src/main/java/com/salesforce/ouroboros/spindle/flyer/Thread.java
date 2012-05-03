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
package com.salesforce.ouroboros.spindle.flyer;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.EventChannel.EventSegment;
import com.salesforce.ouroboros.spindle.Segment;

/**
 * The thread of events on a channel.
 * 
 * “For Fate has wove the thread of life with pain And twins even from the birth
 * are Misery and Man”
 * 
 * @author hhildebrand
 * 
 */
public class Thread {
    private static final Logger log = LoggerFactory.getLogger(Thread.class);

    private final EventChannel  channel;
    private long                currentEventId;
    private long                currentPosition;
    private int                 remainingBytes;
    private Segment             segment;

    public Thread(EventChannel c, long eventId) throws IOException {
        channel = c;
        load(eventId);
    }

    /**
     * Load the current position of the thread
     * 
     * @param eventId
     *            - the event id to track
     * @throws IOException
     */
    public void load(long eventId) throws IOException {
        EventSegment event = channel.eventSegmentFor(eventId);
        currentEventId = eventId;
        segment = event.segment;
        currentPosition = event.offset;
        remainingBytes = EventHeader.payloadLengthOf(currentPosition, segment);
    }

    /**
     * Transfer the thread's event stream to the channel. Continue supplying the
     * event bytes until either the maximum number of events are reached, the
     * maximum number of bytes are reached, or the channel will not accept more
     * bytes
     * 
     * @param maxBytes
     *            - the maximum number of bytes to transfer to the channel
     * @param maxEvents
     *            - the maximum number of events to transfer to the channel
     * @param channel
     *            - the channel to transfer to
     * 
     * @return the number of bytes written to the channel
     * @throws IOException
     */
    public long pull(int maxBytes, int maxEvents, WritableByteChannel channel)
                                                                              throws IOException {
        int txfr = Math.min(maxBytes, remainingBytes);
        long written = segment.transferTo(currentPosition, txfr, channel);
        return written;
    }
}
