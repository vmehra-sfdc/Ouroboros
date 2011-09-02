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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Efficiently multiplexes the case where a very large number of producers are
 * pushing to a single consumer.
 * 
 * @author hhildebrand
 * 
 */
public class Multiplexer<T> {

    public interface Producer<T> {
        T produce();
    }

    private final BlockingQueue<T>           consumer;
    private final Executor                   executor;
    private final AbstractQueue<Producer<T>> producers = new LinkedBlockingDeque<Producer<T>>();
    private final long                       timeout;
    private final TimeUnit                   unit;
    private AtomicBoolean                    running   = new AtomicBoolean();

    public Multiplexer(Executor executor, BlockingQueue<T> consumer,
                       long timeout, TimeUnit unit) {
        this.executor = executor;
        this.consumer = consumer;
        this.timeout = timeout;
        this.unit = unit;
    }

    public void add(Producer<T> producer) {
        producers.add(producer);
    }

    public void remove(Producer<T> producer) {
        producers.remove(producer);
    }

    private void run() {
        while (running.get()) {
            for (Producer<T> producer : producers) {
                T element = producer.produce();
                while (running.get()) {
                    try {
                        consumer.offer(element, timeout, unit);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }
}
