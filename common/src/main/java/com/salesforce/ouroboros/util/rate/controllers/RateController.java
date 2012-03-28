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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.salesforce.ouroboros.util.rate.Controller;
import com.salesforce.ouroboros.util.rate.Predicate;

/**
 * 
 * An implementation of ResponseTimeController that uses a direct adjustment of
 * queue thresholds based on the error in the 90th percentile response time.
 * 
 * @author hhildebrand
 * 
 */
public class RateController implements Controller {
    private volatile double     additiveIncrease       = 0.5;
    private volatile double     highWaterMark          = 1.2D;
    private volatile long       lastSampled            = 0;
    private final ReentrantLock lock                   = new ReentrantLock();
    private volatile double     lowWaterMark           = 0.9D;
    private volatile double     maximum                = 5000.0;
    private volatile double     minimum                = 0.05;
    private volatile double     multiplicativeDecrease = 2;
    private final Predicate     predicate;
    private final AtomicInteger sampleCount            = new AtomicInteger();
    private final int           sampleFrequency;
    private volatile long       sampleRate             = 1000L;
    private volatile double     smoothConstant         = 0.7;
    private volatile double     target;
    private final SampleWindow  window;
    private final double        targetPercentile;

    public RateController(Predicate predicate) {
        this(predicate, 1000, 1, 0.9);
    }

    public RateController(Predicate predicate, double minimumRate,
                          double maximumRate, int windowSize,
                          int sampleFrequency, double targetPercentile) {
        this(predicate, windowSize, sampleFrequency, targetPercentile);
        minimum = minimumRate;
        maximum = maximumRate;
    }

    public RateController(Predicate predicate, int windowSize,
                          int sampleFrequency, double targetPercentile) {
        window = new SampleWindow(windowSize);
        this.predicate = predicate;
        this.sampleFrequency = sampleFrequency;
        this.targetPercentile = targetPercentile;
    }

    @Override
    public boolean accept(int cost, long currentTime) {
        return predicate.accept(cost, currentTime);
    }

    @Override
    public boolean accept(long currentTime) {
        return predicate.accept(currentTime);
    }

    public double getAdditiveIncrease() {
        return additiveIncrease;
    }

    public double getHighWaterMark() {
        return highWaterMark;
    }

    public double getLowWaterMark() {
        return lowWaterMark;
    }

    public double getMaximum() {
        return maximum;
    }

    @Override
    public double getMedianResponseTime() {
        return window.getMedian();
    }

    public double getMinimum() {
        return minimum;
    }

    public double getMultiplicativeDecrease() {
        return multiplicativeDecrease;
    }

    @Override
    public double getResponseTime() {
        return window.getPercentile(0.9);
    }

    public long getSampleRate() {
        return sampleRate;
    }

    public double getSmoothConstant() {
        return smoothConstant;
    }

    @Override
    public double getTarget() {
        return target;
    }

    @Override
    public int getWindow() {
        return window.getWindow();
    }

    @Override
    public void sample(double sample, long currentTime) {
        // Only sample every N batches
        if (sampleCount.incrementAndGet() % sampleFrequency != 0) {
            return;
        }
        ReentrantLock myLock = lock;
        if (!myLock.tryLock()) {
            // Skip sample if locked
            return;
        }
        try {

            if (currentTime - lastSampled < sampleRate) {
                return;
            }
            window.sample(sample);
            lastSampled = currentTime;
            double data = window.getPercentile(targetPercentile);

            if (data < lowWaterMark * target) {
                increaseRate();
            } else if (data > highWaterMark * target) {
                decreaseRate();
            }
        } finally {
            myLock.unlock();
        }
    }

    public void setAdditiveIncrease(double additiveIncrease) {
        this.additiveIncrease = additiveIncrease;
    }

    public void setHighWaterMark(double highWaterMark) {
        this.highWaterMark = highWaterMark;
    }

    public void setLowWaterMark(double lowWaterMark) {
        this.lowWaterMark = lowWaterMark;
    }

    public void setMaximum(double maximum) {
        this.maximum = maximum;
    }

    public void setMinimum(double minimum) {
        this.minimum = minimum;
    }

    public void setMultiplicativeDecrease(double multiplicativeDecrease) {
        this.multiplicativeDecrease = multiplicativeDecrease;
    }

    public void setSampleRate(long sampleRate) {
        this.sampleRate = sampleRate;
    }

    public void setSmoothConstant(double smoothConstant) {
        this.smoothConstant = smoothConstant;
    }

    @Override
    public void setTarget(double targetRate) {
        target = targetRate;
        predicate.setTargetRate(targetRate);
    }

    protected void decreaseRate() {
        if (target > minimum) {
            target /= multiplicativeDecrease;
            if (target < minimum) {
                target = minimum;
            }
            predicate.setTargetRate(target);
        }
    }

    protected void increaseRate() {
        if (target < maximum) {
            target += additiveIncrease;
            if (target > maximum) {
                target = maximum;
            }
            predicate.setTargetRate(target);
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.rate.Controller#reset()
     */
    @Override
    public void reset() {
        window.reset();
    }
}
