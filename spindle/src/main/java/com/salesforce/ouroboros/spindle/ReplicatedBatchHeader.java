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

import java.nio.ByteBuffer;
import java.util.UUID;

import com.salesforce.ouroboros.BatchHeader;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ReplicatedBatchHeader extends BatchHeader {
    private static final int BATCH_OFFSET_OFFSET = BatchHeader.HEADER_SIZE;
    public static final int  HEADER_SIZE         = BATCH_OFFSET_OFFSET + 4;

    public ReplicatedBatchHeader() {
        super();
    }

    public ReplicatedBatchHeader(ByteBuffer b) {
        super(b);
    }

    public ReplicatedBatchHeader(int batchByteLength, int magic, UUID channel,
                                 long timestamp, long batchOffset) {
        super(batchByteLength, magic, channel, timestamp);
        bytes.putLong(BATCH_OFFSET_OFFSET, batchOffset);
    }

    public long getOffset() {
        return bytes.getLong(BATCH_OFFSET_OFFSET);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.BatchHeader#getHeaderSize()
     */
    @Override
    protected int getHeaderSize() {
        return HEADER_SIZE;
    }
}
