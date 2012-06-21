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

import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread safe pooling implementation.
 * 
 * @author hhildebrand
 * 
 */
public class Pool<T> {
    public interface Clearable {
        void clear();
    }

    public interface Factory<T> {
        T newInstance(Pool<T> pool);
    }

    private final ReentrantLock lock      = new ReentrantLock();
    private int                 created   = 0;
    private int                 discarded = 0;
    private final Factory<T>    factory;
    private final String        name;
    private final RingBuffer<T> pool;
    private int                 pooled    = 0;
    private int                 reused    = 0;

    public Pool(String name, Factory<T> factory, int limit) {
        this.name = name;
        pool = new RingBuffer<T>(limit);
        this.factory = factory;
    }

    public T allocate() {
        final ReentrantLock myLock = lock;
        myLock.lock();
        try {
            T allocated = pool.poll();
            if (allocated == null) {
                created++;
                allocated = factory.newInstance(this);
            } else {
                reused++;
            }
            return allocated;
        } finally {
            myLock.unlock();
        }
    }

    public void free(T free) {
        final ReentrantLock myLock = lock;
        myLock.lock();
        try {
            if (!pool.offer(free)) {
                discarded++;
            } else {
                pooled++;
                if (free instanceof Clearable) {
                    ((Clearable) free).clear();
                }
            }
        } finally {
            myLock.unlock();
        }
    }

    /**
     * @return the created
     */
    public int getCreated() {
        return created;
    }

    /**
     * @return the discarded
     */
    public int getDiscarded() {
        return discarded;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public int getPooled() {
        return pooled;
    }

    public int size() {
        return pool.size();
    }

    @Override
    public String toString() {
        return String.format("Pool[%s] size: %s reused: %s created: %s pooled: %s discarded: %s",
                             name, size(), reused, created, pooled, discarded);
    }
}
