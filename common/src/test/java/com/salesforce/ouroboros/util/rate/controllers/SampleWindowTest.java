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
package com.salesforce.ouroboros.util.rate.controllers;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

/**
 * 
 * @author hhildebrand
 * 
 */
public class SampleWindowTest {
    @Test
    public void testNonVarying() {
        int windowSize = 1000;
        SampleWindow window = new SampleWindow(windowSize);
        for (int i = 0; i < 5 * windowSize; i++) {
            window.sample(1);
        }
        assertEquals(1, (int) window.getMedian());
        assertEquals(1, (int) window.getPercentile(0.9));
    }

    @Test
    public void testRandom() {
        Random r = new Random(666);
        int windowSize = 1000;
        ArrayDeque<Integer> deque = new ArrayDeque<Integer>();
        SampleWindow window = new SampleWindow(windowSize);
        for (int i = 0; i < 5 * windowSize; i++) {
            int data = r.nextInt();
            window.sample(data);
            deque.add(data);
            if (deque.size() > windowSize) {
                deque.removeFirst();
            }
        }
        int[] reference = new int[deque.size()];
        int index = 0;
        for (int i : deque) {
            reference[index++] = i;
        }
        Arrays.sort(reference);
        assertEquals(reference[(windowSize / 2)], (int) window.getMedian());
    }
}
