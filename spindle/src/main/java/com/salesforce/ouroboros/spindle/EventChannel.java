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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.replication.ReplicatedBatchHeader;
import com.salesforce.ouroboros.spindle.replication.Replicator;
import com.salesforce.ouroboros.spindle.source.Acknowledger;

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

    public static class AppendSegment {
        public final long    offset;
        public final long    position;
        public final Segment segment;

        public AppendSegment(Segment segment, long offset, long position) {
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

    public enum Role {
        MIRROR, PRIMARY;
    }

    private static class AppendSegmentName {
        public final long   offset;
        public final long   position;
        public final String segment;

        public AppendSegmentName(String segment, long offset, long position) {
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

    private static final Logger         log            = Logger.getLogger(Weaver.class.getCanonicalName());
    private static final FilenameFilter SEGMENT_FILTER = new FilenameFilter() {
                                                           @Override
                                                           public boolean accept(File dir,
                                                                                 String name) {
                                                               return name.endsWith(SEGMENT_SUFFIX);
                                                           }
                                                       };

    public static void deleteDirectory(File directory) {
        for (File n : directory.listFiles()) {
            if (n.isDirectory()) {
                deleteDirectory(n);
            }
            if (!n.delete()) {
                log.warning(String.format("Channel cannot delete: %s",
                                          n.getAbsolutePath()));
            }
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
                                                                offset
                                                                        - homeSegment);
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

    private final File       channel;
    private volatile long    commited;
    private final UUID       id;
    private volatile long    lastTimestamp;
    private final long       maxSegmentSize;
    private volatile long    nextOffset;
    private final Replicator replicator;

    private Role             role;

    public EventChannel(Role role, final UUID channelId, final File root,
                        final long maxSegmentSize, final Replicator replicator) {
        this.role = role;
        id = channelId;
        channel = new File(root, channelId.toString().replace('-', '/'));
        this.maxSegmentSize = maxSegmentSize;

        channel.mkdirs();

        if (channel.exists() && channel.isDirectory()) {
            deleteDirectory(channel);
        } else {
            String msg = String.format("Unable to create channel directory for channel: %s",
                                       channel);
            log.severe(msg);
            throw new IllegalStateException(msg);
        }
        this.replicator = replicator;
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
        segment.force(false);
        nextOffset = offset + batchHeader.getBatchByteLength();
        lastTimestamp = batchHeader.getTimestamp();
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
    public void append(ReplicatedBatchHeader batchHeader, Segment segment,
                       Acknowledger acknowledger) throws IOException {
        append(batchHeader, batchHeader.getOffset(), segment);
        replicator.replicate(batchHeader, this, segment, acknowledger);
    }

    /**
     * Close the channel
     */
    public void close() {
        deleteDirectory(channel);
        if (!channel.delete()) {
            log.severe(String.format("Cannot delete channel root directory: %s",
                                     channel));
        } else {
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Deleted channel root directory: %s",
                                       channel));
            }
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

    public UUID getId() {
        return id;
    }

    /**
     * @return the replicator
     */
    public Replicator getReplicator() {
        return replicator;
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
        File[] segmentFiles = channel.listFiles(SEGMENT_FILTER);
        Arrays.sort(segmentFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        Deque<Segment> segments = new LinkedList<Segment>();
        for (File segmentFile : segmentFiles) {
            try {
                segments.push(new Segment(segmentFile));
            } catch (FileNotFoundException e) {
                String msg = String.format("Cannot find segment file: %s",
                                           segmentFile);
                log.log(Level.SEVERE, msg, e);
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
        return batchHeader.getTimestamp() < lastTimestamp;
    }

    public boolean isMirror() {
        return role == Role.MIRROR;
    }

    public boolean isPrimary() {
        return role == Role.PRIMARY;
    }

    public AppendSegment segmentFor(BatchHeader batchHeader) {
        AppendSegmentName logicalSegment = mappedSegmentFor(nextOffset,
                                                            batchHeader.getBatchByteLength(),
                                                            maxSegmentSize);
        File segmentFile = new File(channel, logicalSegment.segment);
        if (!segmentFile.exists()) {
            try {
                segmentFile.createNewFile();
            } catch (IOException e) {
                String msg = String.format("Cannot create the new segment file: %s",
                                           segmentFile.getAbsolutePath());
                log.log(Level.WARNING, msg, e);
                throw new IllegalStateException(msg);
            }
        }
        try {
            return new AppendSegment(new Segment(segmentFile),
                                     logicalSegment.offset,
                                     logicalSegment.position);
        } catch (FileNotFoundException e) {
            String msg = String.format("The segment file cannot be found, yet was created: %s",
                                       segmentFile.getAbsolutePath());
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
    public AppendSegment segmentFor(long offset) {
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
            return new AppendSegment(new Segment(segment), offset, 0);
        } catch (FileNotFoundException e) {
            String msg = String.format("The segment file cannot be found, yet was created: %s",
                                       segment.getAbsolutePath());
            log.log(Level.WARNING, msg, e);
            throw new IllegalStateException(msg);
        }
    }

    public void setMirror() {
        role = Role.MIRROR;
    }

    public void setPrimary() {
        role = Role.PRIMARY;
    }
}
