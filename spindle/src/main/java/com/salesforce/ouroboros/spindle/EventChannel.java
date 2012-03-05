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
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Node;
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
        public final long    offset;
        public final int     position;
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

    private static final Logger         log            = Logger.getLogger(Weaver.class.getCanonicalName());
    private static final FilenameFilter SEGMENT_FILTER = new FilenameFilter() {
                                                           @Override
                                                           public boolean accept(File dir,
                                                                                 String name) {
                                                               return name.endsWith(SEGMENT_SUFFIX);
                                                           }
                                                       };

    public static void deleteDirectory(File directory) {
        if (directory == null) {
            log.warning(String.format("Attempt to delete a null directory"));
            return;
        }
        File[] list = directory.listFiles();
        if (list != null) {
            for (File n : list) {
                if (n.isDirectory()) {
                    deleteDirectory(n);
                } else if (!n.delete()) {
                    log.warning(String.format("Channel cannot delete: %s",
                                              n.getAbsolutePath()));
                }
            }
        }
        if (!directory.delete()) {
            log.warning(String.format("Channel cannot delete: %s",
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
    private final Node                         partner;
    private volatile Replicator                replicator;
    private Role                               role;
    private final ConcurrentMap<File, Segment> segmentCache;

    public EventChannel(Role role, Node partnerId, final UUID channelId,
                        final File root, final long maxSegmentSize,
                        final Replicator replicator,
                        ConcurrentMap<File, Segment> segmentCache) {
        assert root != null : "Root directory must not be null";
        assert channelId != null : "Channel id must not be null";

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
                log.severe(msg);
                throw new IllegalStateException(msg);
            }
        }
        this.replicator = replicator;
        this.segmentCache = segmentCache;
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
                       Acknowledger acknowledger, SocketChannelHandler handler)
                                                                               throws IOException {
        append(batchHeader, batchHeader.getOffset(), segment);
        if (replicator == null) {
            handler.selectForRead();
        } else {
            replicator.replicate(new EventEntry(batchHeader, this, segment,
                                                acknowledger, handler));
        }
    }

    /**
     * Close the channel
     */
    public void close(Node producerId) {
        log.info(String.format("Closing channel %s on %s, root directory: %s",
                               id, producerId, channel));
        for (File segmentFile : getSegmentFiles()) {
            Segment segment = segmentCache.remove(segmentFile);
            if (segment != null) {
                try {
                    segment.close();
                } catch (IOException e) {
                    log.finest(String.format("Error closing %s", segment));
                }
            }
        }
        deleteDirectory(channel);
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Deleted channel root directory: %s",
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

    public void failOver() {
        failedOver = true;
        role = Role.PRIMARY;
        replicator = null;
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
                segments.push(getCachedSegment(segmentFile));
            } catch (FileNotFoundException e) {
                String msg = String.format("Cannot find segment file: %s",
                                           segmentFile);
                log.log(Level.SEVERE, msg, e);
                throw new IllegalStateException(msg, e);
            }
        }
        return segments;
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

    public void rebalanceAsMirror() {
        failedOver = false;
        role = Role.MIRROR;
        replicator = null;
    }

    public void rebalanceAsPrimary(Replicator replicator) {
        failedOver = false;
        role = Role.PRIMARY;
        this.replicator = replicator;
    }

    public AppendSegment segmentFor(BatchHeader batchHeader) throws IOException {
        AppendSegmentName logicalSegment = mappedSegmentFor(nextOffset,
                                                            batchHeader.getBatchByteLength(),
                                                            maxSegmentSize);
        File segmentFile = new File(channel, logicalSegment.segment);
        return getSegment(logicalSegment.offset, logicalSegment.position,
                          segmentFile);
    }

    /**
     * Answer the segment for the event at the given offset
     * 
     * @param offset
     *            - the offset of the event
     * @return the segment for the event
     * @throws IOException
     */
    public AppendSegment segmentFor(long offset) throws IOException {
        File segment = new File(channel, segmentName(prefixFor(offset,
                                                               maxSegmentSize)));
        return getSegment(offset, -1, segment);
    }

    /**
     * Answer the segment for the event at the given offset
     * 
     * @param offset
     *            - the offset of the event
     * @return the segment for the event
     * @throws IOException
     */
    public AppendSegment segmentFor(long offset, int position)
                                                              throws IOException {
        File segment = new File(channel, segmentName(prefixFor(offset,
                                                               maxSegmentSize)));
        return getSegment(offset, position, segment);
    }

    private AppendSegment getSegment(long offset, int position, File segment)
                                                                             throws IOException {
        return new AppendSegment(getCachedSegment(segment), offset, position);
    }

    private Segment getCachedSegment(File segment) throws FileNotFoundException {
        Segment newSegment = new Segment(segment);
        Segment currentSegment = segmentCache.putIfAbsent(segment, newSegment);
        if (currentSegment == null) {
            currentSegment = newSegment;
            currentSegment.open();
        }
        return currentSegment;
    }

    public void failMirror() {
        replicator = null;
    }
}
