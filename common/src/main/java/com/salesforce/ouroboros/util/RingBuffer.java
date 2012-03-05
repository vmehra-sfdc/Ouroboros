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
package com.salesforce.ouroboros.util;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a fixed size Queue implementation. This class is not thread safe.
 * 
 * @author hhildebrand
 * 
 * @param <T>
 */
public class RingBuffer<T> extends AbstractQueue<T> {

    private int         size = 0;
    private int         head = 0;
    protected final T[] items;
    private int         tail = 0;
    private int         capacity;

    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        this.capacity = capacity;
        items = (T[]) new Object[capacity];
    }

    /* (non-Javadoc)
     * @see java.util.AbstractCollection#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int current = 0;

            @Override
            public boolean hasNext() {
                return current < size;
            }

            @Override
            public T next() {
                if (current == size) {
                    throw new NoSuchElementException();
                }
                return items[(current++ + head) % items.length];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /* (non-Javadoc)
     * @see java.util.Queue#offer(java.lang.Object)
     */
    @Override
    public boolean offer(T value) {
        if (size == capacity) {
            return false;
        }
        items[tail] = value;
        tail = (tail + 1) % items.length;
        size++;
        return true;
    }

    /* (non-Javadoc)
     * @see java.util.Queue#peek()
     */
    @Override
    public T peek() {
        if (size == 0) {
            return null;
        }
        return items[head];
    }

    /* (non-Javadoc)
     * @see java.util.Queue#poll()
     */
    @Override
    public T poll() {
        if (size == 0) {
            return null;
        }
        T item = items[head];
        items[head] = null;
        size--;
        head = (head + 1) % items.length;
        return item;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("[ ");
        for (int i = 0; i < size; i++) {
            buf.append(items[(i + head) % items.length]);
            buf.append(", ");
        }
        buf.append("]");
        return buf.toString();
    }
}