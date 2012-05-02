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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author hhildebrand
 * 
 */
public class Pool<T> {
    public interface Factory<T> {
        T newInstance();
    }

    public interface Linkable<T> {
        T _self();

        Linkable<T> delink();

        Linkable<T> link(Linkable<T> freeList);
    }

    private final Factory<T>    factory;
    private Linkable<T>         freeList;
    private final ReentrantLock freeListLock    = new ReentrantLock();
    private AtomicLong          lifetimeMaxSize = new AtomicLong();
    private final int           limit;
    private AtomicLong          size            = new AtomicLong();

    public Pool(Factory<T> factory, int limit) {
        this.limit = limit;
        this.factory = factory;
    }

    public T allocate() {
        final ReentrantLock lock = freeListLock;
        lock.lock();
        try {
            if (freeList == null) {
                lifetimeMaxSize.set(Math.max(lifetimeMaxSize.get(),
                                             size.incrementAndGet()));
                return factory.newInstance();
            }
            Linkable<T> allocated = freeList;
            freeList = allocated.delink();
            return allocated._self();
        } finally {
            lock.unlock();
        }
    }

    public void free(Linkable<T> free) {
        if (size.get() <= limit) {
            final ReentrantLock lock = freeListLock;
            lock.lock();
            try {
                freeList = free.link(freeList);
                size.decrementAndGet();
            } finally {
                lock.unlock();
            }
        }
    }

    public long size() {
        return size.get();
    }

    public long getLifetimeMaximumSize() {
        return lifetimeMaxSize.get();
    }
}
