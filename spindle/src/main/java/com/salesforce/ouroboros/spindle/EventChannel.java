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
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The representation of the event channel. A channel is a logical collection of
 * segments, identified by a UUID. Events published to the channel are only
 * appended, and only one thread is ever appending to a channel at any given
 * instant in time. The channels keep track of the offset the last committed
 * event, as well as the offset of the last appended event and the timestamp of
 * that event. The channel is responsible for the logic of maintaining the
 * segments, as well as determining whether events are duplicating events
 * already in the channel.
 * 
 * @author hhildebrand
 * 
 */
public class EventChannel {

    public enum Role {
        MIRROR, PRIMARY;
    }

    private static final Logger log            = Logger.getLogger(Weaver.class.getCanonicalName());
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

    private final File       channel;
    private volatile long    commited;
    private volatile long    lastTimestamp;
    private final long       maxSegmentSize;
    private volatile long    nextOffset;
    private final Duplicator replicator;
    private final UUID       tag;

    public EventChannel(final UUID channelTag, final File root,
                        final long maxSegmentSize, final Duplicator replicator) {
        tag = channelTag;
        channel = new File(root, channelTag.toString().replace('-', '/'));
        this.maxSegmentSize = maxSegmentSize;
        if (!channel.mkdirs()) {
            String msg = String.format("Unable to create channel directory for channel: %s",
                                       channel);
            log.severe(msg);
            throw new IllegalStateException(msg);
        }
        this.replicator = replicator;
    }

    public void append(EventHeader header, long offset) {
        nextOffset = offset + header.totalSize();
        lastTimestamp = header.getId();
    }

    /**
     * Mark the appending of the event at the offset in the channel
     * 
     * @param header
     * @param offset
     * @param segment
     */
    public void append(EventHeader header, long offset, Segment segment) {
        append(header, offset);
        replicator.replicate(this, offset, segment, header.totalSize());
    }

    /**
     * Commit the event offset in the channel
     * 
     * @param offset
     */
    public void commit(final long offset) {
        commited = offset;
    }

    public long committed() {
        return commited;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EventChannel)) {
            return false;
        }
        return tag.equals(o);
    }

    /**
     * @return the unique tag of the channel
     */
    public UUID getTag() {
        return tag;
    }

    @Override
    public int hashCode() {
        return tag.hashCode();
    }

    /**
     * Answer true if the header represents a duplicate event in the channel
     * 
     * @param header
     *            - the event header to test
     * @return true if the header represents a duplicate event.
     */
    public boolean isDuplicate(EventHeader header) {
        return header.getId() < lastTimestamp;
    }

    /**
     * Answer true if the offset is the next append offset of the channel
     * 
     * @param offset
     *            - the event offset
     * @return true if the event channel's append offset is equal to the offset
     */
    public boolean isNextAppend(long offset) {
        return nextOffset == offset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    /**
     * Answer the segment that the event can be appended to.
     * 
     * @param header
     *            - the event header
     * @return the Segment to append the event
     */
    public Segment segmentFor(EventHeader header) {
        File segment = new File(channel,
                                appendSegmentNameFor(header.totalSize(),
                                                     maxSegmentSize));
        if (!segment.exists()) {
            try {
                segment.createNewFile();
            } catch (IOException e) {
                String msg = String.format("Cannot create the new segment file: %s",
                                           segment.getAbsolutePath());
                log.log(Level.WARNING, msg, e);
                throw new IllegalStateException(msg);
            }
        }
        try {
            return new Segment(segment);
        } catch (FileNotFoundException e) {
            String msg = String.format("The segment file cannot be found, yet was created: %s",
                                       segment.getAbsolutePath());
            log.log(Level.WARNING, msg, e);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Answer the segment for the event at the given offset
     * 
     * @param offset
     *            - the offset of the event
     * @return the segment for the event
     */
    public Segment segmentFor(long offset) {
        File segment = new File(channel, segmentName(prefixFor(offset,
                                                               maxSegmentSize)));
        if (!segment.exists()) {
            try {
                segment.createNewFile();
            } catch (IOException e) {
                String msg = String.format("Cannot create the new segment file: %s",
                                           segment.getAbsolutePath());
                log.log(Level.WARNING, msg, e);
                throw new IllegalStateException(msg);
            }
        }
        try {
            return new Segment(segment);
        } catch (FileNotFoundException e) {
            String msg = String.format("The segment file cannot be found, yet was created: %s",
                                       segment.getAbsolutePath());
            log.log(Level.WARNING, msg, e);
            throw new IllegalStateException(msg);
        }
    }

    private String appendSegmentNameFor(int eventSize, long maxSegmentSize) {
        return segmentName(segmentFor(nextOffset, eventSize, maxSegmentSize));
    }
}
