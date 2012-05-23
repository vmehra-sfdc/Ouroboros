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
package com.salesforce.ouroboros.spindle;

import java.io.IOException;

import com.salesforce.ouroboros.spindle.flyer.EventSpan;

/**
 * The tuple representing an event at a given concrete offset within a segment
 * 
 * @author hhildebrand
 * 
 */
public class EventSegment {
    /**
     * The translated offset of the event payload in this segment
     */
    public final long    offset;
    /**
     * The segment where the event lives
     */
    public final Segment segment;

    /**
     * @param offset
     * @param segment
     */
    public EventSegment(long offset, Segment segment) {
        this.offset = offset;
        this.segment = segment;
    }

    /**
     * Answer true if the segment contains the logical event offset
     * 
     * @param logicalOffset
     *            -the logical offset of an event
     * @return true if the segment contains the offset, false otherwise
     */
    public boolean contains(long logicalOffset) {
        return segment.contains(logicalOffset);
    }

    /**
     * @return the EventSegment that logically follows the last event in the
     *         current segment
     * @throws IOException
     *             if the segment cannot be retrieved
     */
    public EventSegment followingSegment() throws IOException {
        return new EventSegment(0, segment.nextSegment());
    }

    /**
     * @return the EventSegment containing the event after the event at the
     *         offset in the segment
     * @throws IOException
     *             if we cannot read the event from backing file
     */
    public EventSegment segmentAfter() throws IOException {
        long nextOffset = segment.offsetAfter(offset);
        if (nextOffset < 0) {
            return new EventSegment(0L, segment.nextSegment());
        } else {
            return new EventSegment(nextOffset, segment);
        }
    }

    /**
     * @return the EventSpan containing the events in the segment from the
     *         current offset until the end of the segment, inclusive
     */
    public EventSpan span() {
        return segment.spanFrom(offset);
    }
}