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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author hhildebrand
 * 
 */
public class EventChannel {
    private static final Logger log            = LoggerFactory.getLogger(Wrangler.class);
    private static final String SEGMENT_SUFFIX = ".segment";

    /**
     * Answer the logical segment prefix for the event offset and given maximum
     * segment size
     * 
     * @param offset
     *            - the offset of the event
     * @param maxSegmentSize
     *            - the maximum segment size
     * @return
     */
    public static long prefixFor(long offset, long maxSegmentSize) {
        return (long) Math.floor(offset / maxSegmentSize) * maxSegmentSize;
    }

    /**
     * Answer the logical segment two which the segment belongs
     * 
     * @param offset
     *            - the proposed offset of the event
     * @param eventSize
     *            - the total size of the event
     * @param maxSegmentSize
     *            - the maximum segment size
     * @return
     */
    public static long segmentFor(long offset, int eventSize,
                                  long maxSegmentSize) {
        long homeSegment = prefixFor(offset, maxSegmentSize);
        long endSegment = prefixFor(offset + eventSize, maxSegmentSize);
        return homeSegment != endSegment ? endSegment : homeSegment;
    }

    /**
     * Answer the segment file name for the segment prefix
     * 
     * @param segmentPrefix
     * @return
     */
    public static String segmentName(long segmentPrefix) {
        return Long.toHexString(segmentPrefix).toLowerCase() + SEGMENT_SUFFIX;
    }

    private final long    maxSegmentSize;
    private volatile long nextOffset;
    private volatile long lastTimestamp;
    private final File    channel;
    private volatile long commitedOffset;
    private final UUID    tag;

    public EventChannel(final UUID channelTag, final File root,
                        final long maxSegmentSize) {
        tag = channelTag;
        channel = new File(root, channelTag.toString().replace('-', '/'));
        this.maxSegmentSize = maxSegmentSize;
        if (!channel.mkdirs()) {
            String msg = String.format("Unable to create channel directory for channel: %s",
                                       channel);
            log.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Mark the appending of the event at the offset in the channel
     * 
     * @param offset
     * @param header
     */
    public void append(long offset, EventHeader header) {
        nextOffset = offset + header.totalSize();
        lastTimestamp = header.getTimestamp();
    }

    /**
     * Commit the event offset in the channel
     * 
     * @param offset
     */
    public void commit(final long offset) {
        commitedOffset = offset;
    }

    /**
     * Answer the segment that the event can be appended to.
     * 
     * @param header
     *            - the event header
     * @return the Segment to append the event
     * @throws FileNotFoundException
     *             - if the segment file cannot be found
     */
    public Segment getAppendSegmentFor(EventHeader header)
                                                          throws FileNotFoundException {
        return new Segment(new File(channel,
                                    appendSegmentNameFor(header.totalSize(),
                                                         maxSegmentSize)));
    }

    public long getCommittedOffset() {
        return commitedOffset;
    }

    /**
     * @return the unique tag of the channel
     */
    public UUID getTag() {
        return tag;
    }

    /**
     * Answer true if the header represents a duplicate event in the channel
     * 
     * @param header
     *            - the event header to test
     * @return true if the header represents a duplicate event.
     */
    public boolean isDuplicate(EventHeader header) {
        return header.getTimestamp() < lastTimestamp;
    }

    private String appendSegmentNameFor(int eventSize, long maxSegmentSize) {
        return segmentName(segmentFor(nextOffset, eventSize, maxSegmentSize));
    }
}
