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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Segment.Mode;
import com.salesforce.ouroboros.spindle.flyer.Flyer;
import com.salesforce.ouroboros.spindle.replication.EventEntry;
import com.salesforce.ouroboros.spindle.replication.ReplicatedBatchHeader;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.source.Acknowledger;

/**
 * The representation of the event channel. A channel is a logical collection of
 * segments, identified by a UUID. Events published to the channel are only
 * appended, and only one thread is ever appending to a channel at any given
 * instant in time. The channels keep track of the offset the last committed
 * event, as well as the offset of the last appended event and the sequence
 * number of that event. The channel is responsible for the logic of maintaining
 * the segments, as well as determining whether events are duplicating events
 * already in the channel.
 * 
 * @author hhildebrand
 * 
 */
public class EventChannel {

    public static class AppendSegment {
        // The logical offset within the channel
        public final long    offset;
        // The translated position within the segment
        public final int     position;
        // The segment
        public final Segment segment;

        public AppendSegment(Segment segment, long offset, int position) {
            this.segment = segment;
            this.offset = offset;
            this.position = position;
        }

        @Override
        public String toString() {
            return "AppendSegment [segment=" + segment + ", offset=" + offset
                   + ", position=" + position + "]";
        }
    }

    public static class EventSegment {
        // The translated offset of the event payload in this segment
        public final long    offset;
        // The segment where the event lives
        public final Segment segment;

        /**
         * @param offset
         * @param segment
         */
        public EventSegment(long offset, Segment segment) {
            this.offset = offset;
            this.segment = segment;
        }
    }

    public enum Role {
        MIRROR, PRIMARY;
    }

    private static class AppendSegmentName {
        public final long   offset;
        public final int    position;
        public final String segment;

        public AppendSegmentName(String segment, long offset, int position) {
            this.segment = segment;
            this.offset = offset;
            this.position = position;
        }

        @Override
        public String toString() {
            return "AppendSegmentName [segment=" + segment + ", offset="
                   + offset + "]";
        }
    }

    public static final String          SEGMENT_SUFFIX = ".segment";
    private static final Logger         log            = LoggerFactory.getLogger(EventChannel.class.getCanonicalName());
    private static final FilenameFilter SEGMENT_FILTER = new FilenameFilter() {
                                                           @Override
                                                           public boolean accept(File dir,
                                                                                 String name) {
                                                               return name.endsWith(SEGMENT_SUFFIX);
                                                           }
                                                       };

    public static void deleteDirectory(File directory) {
        if (directory == null) {
            log.warn(String.format("Attempt to delete a null directory"));
            return;
        }
        File[] list = directory.listFiles();
        if (list != null) {
            for (File n : list) {
                if (n.isDirectory()) {
                    deleteDirectory(n);
                } else if (!n.delete()) {
                    log.warn(String.format("Channel cannot delete: %s",
                                           n.getAbsolutePath()));
                }
            }
        }
        if (!directory.delete()) {
            log.warn(String.format("Channel cannot delete: %s",
                                   directory.getAbsolutePath()));
        }

    }

    /**
     * Answer the logical segment to which the segment belongs
     * 
     * @param offset
     *            - the proposed offset of the event
     * @param eventSize
     *            - the total size of the event(s)
     * @param maxSegmentSize
     *            - the maximum segment size
     * @return an array of the segment, the offset and the position within the
     *         segment
     */
    public static AppendSegmentName mappedSegmentFor(long offset,
                                                     int eventSize,
                                                     long maxSegmentSize) {
        long homeSegment = prefixFor(offset, maxSegmentSize);
        long endSegment = prefixFor(offset + eventSize, maxSegmentSize);
        return homeSegment != endSegment ? new AppendSegmentName(
                                                                 segmentName(endSegment),
                                                                 endSegment, 0)
                                        : new AppendSegmentName(
                                                                segmentName(homeSegment),
                                                                offset,
                                                                (int) (offset - homeSegment));
    }

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
     * Answer the segment file name for the segment prefix
     * 
     * @param segmentPrefix
     * @return
     */
    public static String segmentName(long segmentPrefix) {
        return Long.toHexString(segmentPrefix).toLowerCase() + SEGMENT_SUFFIX;
    }

    private final File                         channel;
    private volatile long                      commited;
    private boolean                            failedOver = false;
    private final UUID                         id;
    private volatile long                      lastTimestamp;
    private final long                         maxSegmentSize;
    private volatile long                      nextOffset;
    private volatile Node                      partner;
    private volatile Replicator                replicator;
    private Role                               role;
    private final ConcurrentMap<File, Segment> appendSegmentCache;
    private final ConcurrentMap<File, Segment> readSegmentCache;
    private final Node                         self;

    public EventChannel(Node self, Role role, Node partnerId,
                        final UUID channelId, final File root,
                        final long maxSegmentSize, final Replicator replicator,
                        ConcurrentMap<File, Segment> appendSegmentCache,
                        ConcurrentMap<File, Segment> readSegmentCache) {
        assert self != null : "this node must not be null";
        assert root != null : "Root directory must not be null";
        assert channelId != null : "Channel id must not be null";

        this.self = self;
        this.role = role;
        partner = partnerId;
        id = channelId;
        channel = new File(root, channelId.toString().replace('-', '/'));
        this.maxSegmentSize = maxSegmentSize;

        if (channel.exists() && channel.isDirectory()) {
            log.info(String.format("Clearing channel %s, root directory: %s",
                                   id, channel));
            deleteDirectory(channel);
        } else {
            channel.mkdirs();
            if (!channel.exists() && channel.isDirectory()) {
                String msg = String.format("Unable to create channel directory for channel: %s",
                                           channel);
                log.error(msg);
                throw new IllegalStateException(msg);
            }
        }
        this.replicator = replicator;
        this.appendSegmentCache = appendSegmentCache;
        this.readSegmentCache = readSegmentCache;
    }

    /**
     * Mark the appending of the event at the offset in the channel
     * 
     * @param batchHeader
     * @param offset
     * @param segment
     * @throws IOException
     */
    public void append(BatchHeader batchHeader, long offset, Segment segment)
                                                                             throws IOException {
        nextOffset = offset + batchHeader.getBatchByteLength();
        lastTimestamp = batchHeader.getSequenceNumber();
        if (log.isInfoEnabled()) {
            log.info(String.format("Append %s,%s,%s,%s,%s",
                                   batchHeader.getSequenceNumber(), offset, id,
                                   segment.getSegmentName(), self));
        }
    }

    /**
     * Mark the appending of the event at the offset in the channel, replicating
     * the event batch
     * 
     * @param batchHeader
     * @param segment
     * @param acknowledger
     * @throws IOException
     */
    public void append(EventEntry entry, Acknowledger mirrorAcknowledger)
                                                                         throws IOException {
        ReplicatedBatchHeader batchHeader = entry.getHeader();
        append(batchHeader, batchHeader.getOffset(), entry.getSegment());
        if (replicator == null) {
            entry.getAcknowledger().acknowledge(batchHeader.getChannel(),
                                                batchHeader.getSequenceNumber());
            if (mirrorAcknowledger != null) {
                mirrorAcknowledger.acknowledge(batchHeader.getChannel(),
                                               batchHeader.getSequenceNumber());
            }
            entry.free();
        } else {
            replicator.replicate(entry);
        }
    }

    public AppendSegment appendSegmentFor(BatchHeader batchHeader)
                                                                  throws IOException {
        AppendSegmentName logicalSegment = mappedSegmentFor(nextOffset,
                                                            batchHeader.getBatchByteLength(),
                                                            maxSegmentSize);
        File segmentFile = new File(channel, logicalSegment.segment);
        return getAppendSegment(logicalSegment.offset, logicalSegment.position,
                                segmentFile);
    }

    /**
     * Answer the segment for appending the event at the given offset
     * 
     * @param offset
     *            - the offset of the event
     * @return the segment for the event
     * @throws IOException
     */
    public Segment appendSegmentFor(long offset) throws IOException {
        File segment = new File(channel, segmentName(prefixFor(offset,
                                                               maxSegmentSize)));
        return getCachedAppendSegment(segment);
    }

    /**
     * Answer the segment for the event at the given offset
     * 
     * @param offset
     *            - the offset of the event
     * @return the segment for the event
     * @throws IOException
     */
    public AppendSegment appendSegmentFor(long offset, int position)
                                                                    throws IOException {
        File segment = new File(channel, segmentName(prefixFor(offset,
                                                               maxSegmentSize)));
        return getAppendSegment(offset, position, segment);
    }

    /**
     * Close the channel
     */
    public void close(Node producerId) {
        log.info(String.format("Closing channel %s on %s, root directory: %s",
                               id, producerId, channel));
        for (Segment segment : appendSegmentCache.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                log.trace(String.format("Error closing %s", segment));
            }
        }
        appendSegmentCache.clear();

        for (Segment segment : readSegmentCache.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                log.trace(String.format("Error closing %s", segment));
            }
        }
        readSegmentCache.clear();

        deleteDirectory(channel);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Deleted channel root directory: %s",
                                    channel));
        }
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
        return channel.equals(((EventChannel) o).channel);
    }

    /**
     * Answer the segment for reading the event at the given offset
     * 
     * @param eventId
     *            - the logical offset of the event in the channel
     * @return the EventSegment translating this event within the channel
     * @throws IOException
     */
    public EventSegment eventSegmentFor(long eventId) throws IOException {
        long homeSegment = prefixFor(eventId, maxSegmentSize);
        return new EventSegment(
                                EventHeader.translateToPayload(eventId
                                                               - homeSegment),
                                getCachedReadSegment(new File(
                                                              channel,
                                                              segmentName(homeSegment))));
    }

    public void failMirror() {
        replicator = null;
    }

    public void failOver() {
        failedOver = true;
        role = Role.PRIMARY;
        replicator = null;
    }

    public Segment getCachedReadSegment(File segment) throws IOException {
        Segment newSegment = new Segment(this, segment, Mode.READ);
        Segment currentSegment = readSegmentCache.putIfAbsent(segment,
                                                              newSegment);
        if (currentSegment == null) {
            currentSegment = newSegment;
        } else {
            newSegment.close();
        }
        return currentSegment;
    }

    public UUID getId() {
        return id;
    }

    public Node[] getOriginalMapping(Node self) {
        switch (role) {
            case PRIMARY: {
                if (failedOver) {
                    return new Node[] { partner, self };
                }
                return new Node[] { self, partner };
            }
            case MIRROR: {
                return new Node[] { partner, self };
            }
            default:
                throw new IllegalStateException("Invalid role: " + role);
        }
    }

    public Node getPartnerId() {
        return partner;
    }

    public Role getRole() {
        return role;
    }

    /**
     * @return the deque of segments to be replicated. The segments are sorted
     *         in increasing offset order
     * @throws IllegalStateException
     *             if a valid segment cannot be returned because the file is not
     *             found
     */
    public Deque<Segment> getSegmentStack() {
        File[] segmentFiles = getSegmentFiles();
        Deque<Segment> segments = new LinkedList<Segment>();
        for (File segmentFile : segmentFiles) {
            try {
                segments.push(getCachedReadSegment(segmentFile));
            } catch (IOException e) {
                String msg = String.format("Cannot find segment file: %s",
                                           segmentFile);
                log.error(msg, e);
                throw new IllegalStateException(msg, e);
            }
        }
        return segments;
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }

    /**
     * Answer true if the batch header represents a duplicate series of events
     * in the channel
     * 
     * @param batchHeader
     *            - the batch header to test
     * @return true if the header represents a duplicate series of events.
     */
    public boolean isDuplicate(BatchHeader batchHeader) {
        return batchHeader.getSequenceNumber() < lastTimestamp;
    }

    public boolean isMirror() {
        return role == Role.MIRROR;
    }

    public boolean isPrimary() {
        return role == Role.PRIMARY;
    }

    public void rebalanceAsMirror() {
        failedOver = false;
        role = Role.MIRROR;
        replicator = null;
    }

    public void rebalanceAsPrimary(Node partner, Replicator replicator,
                                   Node self) {
        assert partner != null;
        this.partner = partner;
        assert replicator != null : String.format("Replicator to %s does not exist on %s",
                                                  partner, self);
        failedOver = false;
        role = Role.PRIMARY;
        this.replicator = replicator;
    }

    private AppendSegment getAppendSegment(long offset, int position,
                                           File segment) throws IOException {
        return new AppendSegment(getCachedAppendSegment(segment), offset,
                                 position);
    }

    private Segment getCachedAppendSegment(File segment) throws IOException {
        Segment newSegment = new Segment(this, segment, Mode.APPEND);
        Segment currentSegment = appendSegmentCache.putIfAbsent(segment,
                                                                newSegment);
        if (currentSegment == null) {
            currentSegment = newSegment;
        } else {
            newSegment.close();
        }
        return currentSegment;
    }

    private File[] getSegmentFiles() {
        File[] segmentFiles = channel.listFiles(SEGMENT_FILTER);
        if (segmentFiles == null) {
            return new File[0];
        }
        Arrays.sort(segmentFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o2.getName().compareTo(o1.getName());
            }
        });
        return segmentFiles;
    }

    /**
     * @param flyer
     * @param lastEventId
     */
    public void subscribe(Flyer flyer, long lastEventId) {
        // TODO Auto-generated method stub

    }
}
