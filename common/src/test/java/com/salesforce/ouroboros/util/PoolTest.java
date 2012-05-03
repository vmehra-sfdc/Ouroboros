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

import static junit.framework.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.salesforce.ouroboros.util.Pool.Factory;

/**
 * @author hhildebrand
 * 
 */
public class PoolTest {
    @Test
    public void testFactory() {
        final AtomicInteger count = new AtomicInteger(0);
        Pool<String> test = new Pool<String>("test-me", new Factory<String>() {
            @Override
            public String newInstance(Pool<String> pool) {
                return String.format("created: ", count.incrementAndGet());
            }
        }, 100);

        for (int i = 0; i < 100; i++) {
            assertEquals(String.format("created: ", i), test.allocate());
        }

        assertEquals(100, test.getCreated());

    }

    @Test
    public void testFree() {
        final AtomicInteger count = new AtomicInteger(0);
        Pool<String> test = new Pool<String>("test-me", new Factory<String>() {
            @Override
            public String newInstance(Pool<String> pool) {
                return String.format("created: ", count.incrementAndGet());
            }
        }, 100);

        for (int i = 0; i < 100; i++) {
            test.free(String.format("freed: ", i));
        }
        assertEquals(0, test.getCreated());
        assertEquals(100, test.getPooled());

        test.free(String.format("freed: ", 100));

        assertEquals(100, test.getPooled());
        assertEquals(100, test.size());
        assertEquals(1, test.getDiscarded());

        for (int i = 0; i < 100; i++) {
            assertEquals(String.format("freed: ", i), test.allocate());
        }

        assertEquals(0, test.size());
        assertEquals(0, test.getCreated());

        assertEquals(String.format("created: ", 0), test.allocate());

        assertEquals(1, test.getCreated());
        assertEquals(0, test.size());
    }
}
