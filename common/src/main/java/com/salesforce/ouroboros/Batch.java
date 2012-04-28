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
package com.salesforce.ouroboros;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Batch extends BatchIdentity {
    private static final double ONE_BILLION = 1000000000D;

    public final BatchHeader    header      = new BatchHeader();
    public ByteBuffer           batch;
    private long                created;
    private long                interval;
    private Batch               next;
    private AtomicInteger       reuseCount  = new AtomicInteger(0);

    public Batch() {
    }

    /**
     * @param mirror
     * @param channel
     * @param sequenceNumber
     * @param emptyList
     */
    public Batch(Node mirror, UUID channel, long sequenceNumber,
                 List<ByteBuffer> events) {
        set(mirror, channel, sequenceNumber, events);
    }

    /**
     * @return
     */
    public long batchByteSize() {
        return batch.capacity() + BatchHeader.HEADER_BYTE_SIZE;
    }

    public Batch delink() {
        Batch current = next;
        next = null;
        System.out.println("Incing!");
        reuseCount.incrementAndGet();
        return current;
    }

    /**
     * @return the interval, in milliseconds, between when the batch was
     *         submitted and when it was acknowledged
     */
    public long getInterval() {
        return interval;
    }

    public Batch link(Batch h) {
        next = h;
        reuseCount.incrementAndGet();
        return this;
    }

    /**
     * 
     */
    public void markInterval() {
        if (interval < 0) {
            interval = Math.max(1, System.nanoTime() - created);
        }
    }

    /**
     * @return the transfer rate in bytes per second of this batch.
     */
    public double rate() {
        return batchByteSize() * ONE_BILLION / interval;
    }

    /**
     * @param node
     */
    public void resetMirror(Node node) {
        header.resetMirror(node);
    }

    public void rewind() {
        header.rewind();
        batch.rewind();
    }

    public void set(Node mirror, UUID channel, long sequenceNumber,
                    Collection<ByteBuffer> events) {
        assert next == null : "ruh roh";
        set(channel, sequenceNumber);
        assert events != null : "events must not be null";
        int totalSize = 0;

        for (ByteBuffer event : events) {
            event.rewind();
            totalSize += EventHeader.HEADER_BYTE_SIZE + event.remaining();
        }

        if (batch == null || batch.capacity() < totalSize) {
            batch = ByteBuffer.allocateDirect(totalSize);
        } else {
            batch.limit(totalSize);
            batch.rewind();
        }

        header.set(mirror, totalSize, BatchHeader.MAGIC, channel,
                   sequenceNumber);
        header.rewind();

        for (ByteBuffer event : events) {
            event.rewind();
            EventHeader.append(BatchHeader.MAGIC, event, batch);
        }

        assert batch.remaining() == 0;
        batch.rewind();
        created = System.nanoTime();
        interval = -1;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("Batch:%s [#events=%s, created=%s, channel=%s, sequenceNumber=%s]",
                             reuseCount.get(), batch.capacity(), created,
                             channel, sequenceNumber);
    }
}
