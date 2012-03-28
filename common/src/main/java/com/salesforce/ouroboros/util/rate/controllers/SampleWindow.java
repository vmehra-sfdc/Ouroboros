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

import com.hellblazer.jackal.util.SkipList;
import com.hellblazer.jackal.util.Window;

/**
 * 
 * @author hhildebrand
 * 
 */
public class SampleWindow extends Window {
    private final SkipList sorted = new SkipList();
    private final int      window;

    public SampleWindow(int windowSize) {
        super(windowSize);
        window = windowSize;
    }

    public double getMedian() {
        if (count == 0) {
            throw new IllegalStateException(
                                            "Must have at least one sample to calculate the median");
        }
        return sorted.get(sorted.size() / 2);
    }

    public double getPercentile(double percentile) {
        if (count == 0) {
            throw new IllegalStateException(
                                            "Must have at least one sample to calculate the percentile");
        }
        return sorted.get((int) ((sorted.size() - 1) * percentile));
    }

    public int getWindow() {
        return window;
    }

    /**
     * Reset the state of the receiver
     */
    @Override
    public void reset() {
        super.reset();
        sorted.reset();
    }

    public void sample(double sample) {
        sorted.add(sample);
        if (count == samples.length) {
            sorted.remove(removeFirst());
        }
        addLast(sample);
    }
}
