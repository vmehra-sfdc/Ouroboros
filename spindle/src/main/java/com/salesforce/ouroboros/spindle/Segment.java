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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.InterruptibleChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;

import com.salesforce.ouroboros.EventHeader;
import com.salesforce.ouroboros.EventSpan;

/**
 * Segments are the ultimate repository of events, corresponding to files within
 * the channel. Due to the bogosity in the way that FileChannels are linked with
 * the instances of streams, we have to have an abstraction which wrappers both
 * the channel and the linked stream, because the finalization of the stream
 * will close the file channel's handle, which isn't good. Thus, this class is
 * really nothing more than a huge delegator to the FileChannel member, keeping
 * around the random access file so it won't be GC'd.
 * 
 * @author hhildebrand
 * 
 */
public class Segment implements Channel, InterruptibleChannel, ByteChannel,
        GatheringByteChannel, ScatteringByteChannel, Cloneable {

    public static enum Mode {
        APPEND, READ;
    }

    private final EventChannel channel;
    private final File         file;
    private final FileChannel  fileChannel;

    public Segment(EventChannel channel, File file, Mode mode)
                                                              throws IOException {
        this.file = file;
        this.channel = channel;
        switch (mode) {
            case APPEND: {
                fileChannel = FileChannel.open(file.toPath(), CREATE, WRITE,
                                               APPEND);
                break;
            }
            case READ: {
                fileChannel = FileChannel.open(file.toPath(), READ);
                break;
            }
            default: {
                throw new IllegalArgumentException(
                                                   String.format("Unknown segment mode: %s",
                                                                 mode));
            }
        }
    }

    /**
     * @throws IOException
     * @see java.nio.channels.spi.AbstractInterruptibleChannel#close()
     */
    @Override
    public final void close() throws IOException {
        fileChannel.close();
    }

    /**
     * Answer true if the receiver contains the logical event offset
     * 
     * @param logicalOffset
     *            -the logical offset of an event
     * @return true if the receiver contains the offset, false otherwise
     */
    public boolean contains(long logicalOffset) {
        return getPrefix() == channel.segmentPrefixFor(logicalOffset);
    }

    /**
     * @param obj
     * @return
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Segment)) {
            return false;
        }
        return file.equals(((Segment) obj).file);
    }

    /**
     * @param paramBoolean
     * @throws IOException
     * @see java.nio.channels.FileChannel#force(boolean)
     */
    public void force(boolean paramBoolean) throws IOException {
        fileChannel.force(paramBoolean);
    }

    public EventChannel getEventChannel() {
        return channel;
    }

    /**
     * @return the physical file that backs this segment
     */
    public File getFile() {
        return file;
    }

    /*
     * Answer the prefix of this segment
     */
    public long getPrefix() {
        String name = file.getName();
        int index = name.indexOf(EventChannel.SEGMENT_SUFFIX);
        if (index == -1) {
            throw new IllegalStateException(
                                            String.format("Unable to find segment suffix in segment file name: %s",
                                                          file.getAbsolutePath()));
        }
        return Long.parseLong(name.substring(0, index), 16);
    }

    /**
     * @return the String name of the segment file
     */
    public String getSegmentName() {
        return file.getName().substring(0, file.getName().indexOf('.'));
    }

    /**
     * @return
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return file.hashCode();
    }

    /**
     * @return
     * @see java.nio.channels.spi.AbstractInterruptibleChannel#isOpen()
     */
    @Override
    public final boolean isOpen() {
        return fileChannel.isOpen();
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#lock()
     */
    public final FileLock lock() throws IOException {
        return fileChannel.lock();
    }

    /**
     * @param position
     * @param size
     * @param shared
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#lock(long, long, boolean)
     */
    public FileLock lock(long position, long size, boolean shared)
                                                                  throws IOException {
        return fileChannel.lock(position, size, shared);
    }

    /**
     * @param mode
     * @param position
     * @param size
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode,
     *      long, long)
     */
    public MappedByteBuffer map(MapMode mode, long position, long size)
                                                                       throws IOException {
        return fileChannel.map(mode, position, size);
    }

    /**
     * @return the next segment after the receiver in the logical event channel
     * @throws IOException
     *             if we cannot retrieve the segment
     */
    public Segment nextSegment() throws IOException {
        return channel.segmentAfter(this);
    }

    /**
     * Return the offset of the next event after the offset in the receiver, or
     * -1 if this segment does not host the next event
     * 
     * @param offset
     *            - the offset of the indicated event
     * @return the offset of the next event after the offset in the receiver, or
     *         -1 if this segment does not host the next event
     * @throws IOException
     *             if we are unable to read the event information from backing
     *             file
     */
    public long offsetAfter(long offset) throws IOException {
        long next = offset + EventHeader.totalLengthOf(offset, this);
        if (next > file.length()) {
            return -1;
        } else {
            return next;
        }
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#position()
     */
    public long position() throws IOException {
        return fileChannel.position();
    }

    /**
     * @param newPosition
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#position(long)
     */
    public FileChannel position(long newPosition) throws IOException {
        return fileChannel.position(newPosition);
    }

    /**
     * @param dst
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer)
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return fileChannel.read(dst);
    }

    /**
     * @param dst
     * @param position
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer, long)
     */
    public int read(ByteBuffer dst, long position) throws IOException {
        return fileChannel.read(dst, position);
    }

    /**
     * @param dsts
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer[])
     */
    @Override
    public final long read(ByteBuffer[] dsts) throws IOException {
        return fileChannel.read(dsts);
    }

    /**
     * @param dsts
     * @param offset
     * @param length
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer[], int, int)
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
                                                               throws IOException {
        return fileChannel.read(dsts, offset, length);
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#size()
     */
    public long size() throws IOException {
        return fileChannel.size();
    }

    /**
     * @param offset
     *            - the concrete offset of the first event within this segment
     * @return the EventSpan containing the events in the segment from the
     *         indicated concrete offset through the end of the segment,
     *         inclusive
     */
    public EventSpan spanFrom(long offset) {
        return new EventSpan(getPrefix() + offset, channel.getId(), offset,
                             (int) (file.length() - offset));
    }

    /**
     * @return
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("Segment[%s]", file);
    }

    /**
     * @param src
     * @param position
     * @param count
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#transferFrom(java.nio.channels.ReadableByteChannel,
     *      long, long)
     */
    public long transferFrom(ReadableByteChannel src, long position, long count)
                                                                                throws IOException {
        return fileChannel.transferFrom(src, position, count);
    }

    /**
     * @param position
     * @param count
     * @param target
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#transferTo(long, long,
     *      java.nio.channels.WritableByteChannel)
     */
    public long transferTo(long position, long count, WritableByteChannel target)
                                                                                 throws IOException {
        return fileChannel.transferTo(position, count, target);
    }

    /**
     * @param size
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#truncate(long)
     */
    public FileChannel truncate(long size) throws IOException {
        return fileChannel.truncate(size);
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#tryLock()
     */
    public final FileLock tryLock() throws IOException {
        return fileChannel.tryLock();
    }

    /**
     * @param position
     * @param size
     * @param shared
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#tryLock(long, long, boolean)
     */
    public FileLock tryLock(long position, long size, boolean shared)
                                                                     throws IOException {
        return fileChannel.tryLock(position, size, shared);
    }

    /**
     * @param src
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer)
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return fileChannel.write(src);
    }

    /**
     * @param src
     * @param position
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer, long)
     */
    public int write(ByteBuffer src, long position) throws IOException {
        return fileChannel.write(src, position);
    }

    /**
     * @param srcs
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer[])
     */
    @Override
    public final long write(ByteBuffer[] srcs) throws IOException {
        return fileChannel.write(srcs);
    }

    /**
     * @param srcs
     * @param offset
     * @param length
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer[], int, int)
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
                                                                throws IOException {
        return fileChannel.write(srcs, offset, length);
    }
}
