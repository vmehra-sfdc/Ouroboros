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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

/**
 * @author hhildebrand
 * 
 */
public class RingBufferTest {
    @Test
    public void testOffer() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals("Invalid size", i, test.size());
            assertTrue(test.offer(String.format("Offer: %s", i)));
        }
        assertEquals("Invalid size", 1000, test.size());
        assertFalse((test.offer(String.format("Offer: %s", 1001))));
    }

    @Test
    public void testPoll() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            test.add(String.format("Add: %s", i));
        }
        for (int i = 0; i < 1000; i++) {
            assertEquals("Invalid size", 1000 - i, test.size());
            assertEquals(String.format("Add: %s", i), test.poll());
        }
        assertNull(test.poll());
    }

    @Test
    public void testAccordian() {
        Random r = new Random(0x666);
        RingBuffer<Integer> test = new RingBuffer<Integer>(1000);
        for (int i = 0; i < 500; i++) {
            test.add(i);
        }
        int count = test.size();
        for (int i = 0; i < 10000; i++) {
            if (r.nextBoolean()) {
                assertTrue(test.offer(i));
                count++;
                assertEquals(count, test.size());
            } else {
                assertNotNull(test.poll());
                count--;
                assertEquals(count, test.size());
            }
        }
        assertEquals(count, test.size());
    }
}
