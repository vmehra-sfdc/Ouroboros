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
import java.io.RandomAccessFile;
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

/**
 * 
 * @author hhildebrand
 * 
 */
public class Segment implements Channel, InterruptibleChannel, ByteChannel,
        GatheringByteChannel, ScatteringByteChannel, Cloneable {
    private volatile FileChannel      channel;
    private final File                file;
    private volatile RandomAccessFile raf;

    public Segment(File file) throws FileNotFoundException {
        this.file = file;
        open();
    }

    @Override
    public Segment clone() {
        try {
            return new Segment(file);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Segment file does not exist", e);
        }
    }

    /**
     * @throws IOException
     * @see java.nio.channels.spi.AbstractInterruptibleChannel#close()
     */
    @Override
    public final void close() throws IOException {
        channel.close();
        raf.close();
    }

    /**
     * @param obj
     * @return
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        return channel.equals(obj);
    }

    /**
     * @param paramBoolean
     * @throws IOException
     * @see java.nio.channels.FileChannel#force(boolean)
     */
    public void force(boolean paramBoolean) throws IOException {
        channel.force(paramBoolean);
    }

    /**
     * @return
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return channel.hashCode();
    }

    /**
     * @return
     * @see java.nio.channels.spi.AbstractInterruptibleChannel#isOpen()
     */
    @Override
    public final boolean isOpen() {
        return channel.isOpen();
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#lock()
     */
    public final FileLock lock() throws IOException {
        return channel.lock();
    }

    /**
     * @param paramLong1
     * @param paramLong2
     * @param paramBoolean
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#lock(long, long, boolean)
     */
    public FileLock lock(long paramLong1, long paramLong2, boolean paramBoolean)
                                                                                throws IOException {
        return channel.lock(paramLong1, paramLong2, paramBoolean);
    }

    /**
     * @param paramMapMode
     * @param paramLong1
     * @param paramLong2
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode,
     *      long, long)
     */
    public MappedByteBuffer map(MapMode paramMapMode, long paramLong1,
                                long paramLong2) throws IOException {
        return channel.map(paramMapMode, paramLong1, paramLong2);
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#position()
     */
    public long position() throws IOException {
        return channel.position();
    }

    /**
     * @param paramLong
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#position(long)
     */
    public FileChannel position(long paramLong) throws IOException {
        return channel.position(paramLong);
    }

    /**
     * @param paramByteBuffer
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer)
     */
    @Override
    public int read(ByteBuffer paramByteBuffer) throws IOException {
        return channel.read(paramByteBuffer);
    }

    /**
     * @param paramByteBuffer
     * @param paramLong
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer, long)
     */
    public int read(ByteBuffer paramByteBuffer, long paramLong)
                                                               throws IOException {
        return channel.read(paramByteBuffer, paramLong);
    }

    /**
     * @param dsts
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer[])
     */
    @Override
    public final long read(ByteBuffer[] dsts) throws IOException {
        return channel.read(dsts);
    }

    /**
     * @param paramArrayOfByteBuffer
     * @param paramInt1
     * @param paramInt2
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer[], int, int)
     */
    @Override
    public long read(ByteBuffer[] paramArrayOfByteBuffer, int paramInt1,
                     int paramInt2) throws IOException {
        return channel.read(paramArrayOfByteBuffer, paramInt1, paramInt2);
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#size()
     */
    public long size() throws IOException {
        return channel.size();
    }

    /**
     * @return
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return channel.toString();
    }

    /**
     * @param paramReadableByteChannel
     * @param paramLong1
     * @param paramLong2
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#transferFrom(java.nio.channels.ReadableByteChannel,
     *      long, long)
     */
    public long transferFrom(ReadableByteChannel paramReadableByteChannel,
                             long paramLong1, long paramLong2)
                                                              throws IOException {
        return channel.transferFrom(paramReadableByteChannel, paramLong1,
                                    paramLong2);
    }

    /**
     * @param paramLong1
     * @param paramLong2
     * @param paramWritableByteChannel
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#transferTo(long, long,
     *      java.nio.channels.WritableByteChannel)
     */
    public long transferTo(long paramLong1, long paramLong2,
                           WritableByteChannel paramWritableByteChannel)
                                                                        throws IOException {
        return channel.transferTo(paramLong1, paramLong2,
                                  paramWritableByteChannel);
    }

    /**
     * @param paramLong
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#truncate(long)
     */
    public FileChannel truncate(long paramLong) throws IOException {
        return channel.truncate(paramLong);
    }

    /**
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#tryLock()
     */
    public final FileLock tryLock() throws IOException {
        return channel.tryLock();
    }

    /**
     * @param paramLong1
     * @param paramLong2
     * @param paramBoolean
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#tryLock(long, long, boolean)
     */
    public FileLock tryLock(long paramLong1, long paramLong2,
                            boolean paramBoolean) throws IOException {
        return channel.tryLock(paramLong1, paramLong2, paramBoolean);
    }

    /**
     * @param paramByteBuffer
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer)
     */
    @Override
    public int write(ByteBuffer paramByteBuffer) throws IOException {
        return channel.write(paramByteBuffer);
    }

    /**
     * @param paramByteBuffer
     * @param paramLong
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer, long)
     */
    public int write(ByteBuffer paramByteBuffer, long paramLong)
                                                                throws IOException {
        return channel.write(paramByteBuffer, paramLong);
    }

    /**
     * @param srcs
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer[])
     */
    @Override
    public final long write(ByteBuffer[] srcs) throws IOException {
        return channel.write(srcs);
    }

    /**
     * @param paramArrayOfByteBuffer
     * @param paramInt1
     * @param paramInt2
     * @return
     * @throws IOException
     * @see java.nio.channels.FileChannel#write(java.nio.ByteBuffer[], int, int)
     */
    @Override
    public long write(ByteBuffer[] paramArrayOfByteBuffer, int paramInt1,
                      int paramInt2) throws IOException {
        return channel.write(paramArrayOfByteBuffer, paramInt1, paramInt2);
    }

    private void open() throws FileNotFoundException {
        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
    }
}
