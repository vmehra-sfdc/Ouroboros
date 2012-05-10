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
package com.salesforce.ouroboros.spindle.replication;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Node;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ReplicatedBatchHeader extends BatchHeader {
    private static final int BATCH_OFFSET_OFFSET   = BatchHeader.HEADER_BYTE_SIZE;
    public static final int  BATCH_POSITION_OFFSET = BATCH_OFFSET_OFFSET + 8;
    public static final int  HEADER_SIZE           = BATCH_POSITION_OFFSET + 4;

    private static void set(ByteBuffer headerBytes, long offset, int position,
                            ByteBuffer replicatedHeader) {
        headerBytes.rewind();
        replicatedHeader.rewind();
        replicatedHeader.put(headerBytes);
        replicatedHeader.putLong(BATCH_OFFSET_OFFSET, offset);
        replicatedHeader.putInt(BATCH_POSITION_OFFSET, position);
        replicatedHeader.rewind();
    }

    public ReplicatedBatchHeader() {
        super();
    }

    public ReplicatedBatchHeader(ByteBuffer b) {
        super(b);
    }

    public ReplicatedBatchHeader(Node mirror, int batchByteLength, int magic,
                                 UUID channel, long sequenceNumber,
                                 long batchOffset, int batchPosition) {
        super(mirror, batchByteLength, magic, channel, sequenceNumber);
        getBytes().putLong(BATCH_OFFSET_OFFSET, batchOffset);
        getBytes().putInt(BATCH_OFFSET_OFFSET, batchPosition);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.BatchHeader#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof ReplicatedBatchHeader) {
            ReplicatedBatchHeader b = (ReplicatedBatchHeader) o;
            return b.getMagic() == getMagic()
                   && b.getBatchByteLength() == getBatchByteLength()
                   && b.getSequenceNumber() == b.getSequenceNumber()
                   && b.getOffset() == getOffset()
                   && b.getChannel().equals(getChannel());
        }
        return false;
    }

    public long getOffset() {
        return getBytes().getLong(BATCH_OFFSET_OFFSET);
    }

    public int getPosition() {
        return getBytes().getInt(BATCH_POSITION_OFFSET);
    }

    @Override
    public String toString() {
        return String.format("ReplicatedBatchHeader[magic=%s, sequenceNumber=%s, length=%s, channel=%s offset=%s position=%s]",
                             getMagic(), getSequenceNumber(),
                             getBatchByteLength(), getChannel(), getOffset(),
                             getPosition());
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.BatchHeader#getHeaderSize()
     */
    @Override
    protected int getHeaderSize() {
        return HEADER_SIZE;
    }

    /**
     * @param header
     * @param offset
     * @param startPosition
     */
    public void set(BatchHeader header, long offset, int position) {
        set(header.getBytes(), offset, position, bytes);
    }
}
