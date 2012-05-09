/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.util;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Provides a direct buffer cache. Not thread safe, intended to be put into
 * thread local.
 * 
 * @author hhildebrand
 * 
 */
public class MappedBufferCache {
    private final MappedByteBuffer[] buffers;
    private int                      count = 0;
    private int                      start = 0;

    public MappedBufferCache() {
        buffers = new MappedByteBuffer[8];
    }

    public MappedByteBuffer get(int capacity) {
        if (count == 0) {
            return (MappedByteBuffer) ByteBuffer.allocateDirect(capacity);
        }
        MappedByteBuffer buffer = buffers[start];
        if (buffer.capacity() < capacity) {
            buffer = null;
            int i = start;
            while ((i = next(i)) != start) {
                MappedByteBuffer candidate = buffers[i];
                if (candidate == null)
                    break;
                if (candidate.capacity() >= capacity) {
                    buffer = candidate;
                    break;
                }
            }
            if (buffer == null) {
                if (!isEmpty()) {
                    Utils.unmap(removeFirst());
                }
                return (MappedByteBuffer) ByteBuffer.allocateDirect(capacity);
            }
            buffers[i] = buffers[start];
        }

        buffers[start] = null;
        start = next(start);
        count -= 1;

        buffer.rewind();
        buffer.limit(capacity);
        return buffer;
    }

    private boolean isEmpty() {
        return (count == 0);
    }

    public void recycle(MappedByteBuffer buffer) {
        MappedByteBuffer mapped = (MappedByteBuffer) buffer;
        if (count < 8) {
            int i = (start + count) % 8;
            Utils.unmap(buffers[i]);
            buffers[i] = mapped;
            count += 1;
        } else {
            Utils.unmap(mapped);
        }
    }

    private MappedByteBuffer removeFirst() {
        assert (count > 0);
        MappedByteBuffer buffer = buffers[start];
        buffers[start] = null;
        start = next(start);
        count -= 1;
        return buffer;
    }

    private int next(int paramInt) {
        return (paramInt + 1) % 8;
    }
}
