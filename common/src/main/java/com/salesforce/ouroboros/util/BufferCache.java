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

/**
 * Provides a direct buffer cache. Not thread safe, intended to be put into
 * thread local.
 * 
 * @author hhildebrand
 * 
 */
public class BufferCache {
    private final ByteBuffer[] buffers;
    private int                count;
    private int                start;

    public BufferCache() {
        this.buffers = new ByteBuffer[8];
    }

    public ByteBuffer get(int capacity) {
        if (this.count == 0) {
            return ByteBuffer.allocate(capacity);
        }
        ByteBuffer buffer = this.buffers[this.start];
        if (buffer.capacity() < capacity) {
            buffer = null;
            int i = this.start;
            while ((i = next(i)) != this.start) {
                ByteBuffer candidate = this.buffers[i];
                if (candidate == null)
                    break;
                if (candidate.capacity() >= capacity) {
                    buffer = candidate;
                    break;
                }
            }
            if (buffer == null) {
                if (!isEmpty()) {
                    removeFirst();
                }
                return null;
            }
            this.buffers[i] = this.buffers[this.start];
        }

        this.buffers[this.start] = null;
        this.start = next(this.start);
        this.count -= 1;

        buffer.rewind();
        buffer.limit(capacity);
        return buffer;
    }

    private boolean isEmpty() {
        return (this.count == 0);
    }

    public void add(ByteBuffer buffer) {
        if (this.count < 8) {
            this.start = (this.start + 7) % 8;
            this.buffers[this.start] = buffer;
            this.count += 1;
        }
    }

    public void recycle(ByteBuffer buffer) {
        if (this.count < 8) {
            int i = (this.start + this.count) % 8;
            this.buffers[i] = buffer;
            this.count += 1;
        }
    }

    private ByteBuffer removeFirst() {
        assert (this.count > 0);
        ByteBuffer buffer = this.buffers[this.start];
        this.buffers[this.start] = null;
        this.start = next(this.start);
        this.count -= 1;
        return buffer;
    }

    private int next(int paramInt) {
        return (paramInt + 1) % 8;
    }
}
