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

import java.util.concurrent.locks.ReentrantLock;

import com.salesforce.ouroboros.util.rate.Controller;
import com.salesforce.ouroboros.util.rate.Predicate;
import com.salesforce.ouroboros.util.rate.SampleWindow;

/**
 * 
 * An implementation of ResponseTimeController that uses a direct adjustment of
 * queue thresholds based on the error in the 90th percentile response time.
 * 
 * @author hhildebrand
 * 
 */
public class RateController implements Controller {
    private final Predicate     predicate;
    private volatile double     maximum                = 5000.0;
    private volatile double     minimum                = 0.05;
    private volatile double     highWaterMark          = 1.2D;
    private volatile long       lastSampled            = System.currentTimeMillis();
    private volatile double     lowWaterMark           = 0.9D;
    private volatile long       sampleRate             = 1000L;
    private volatile double     smoothConstant         = 0.7;
    private volatile double     target;
    private final SampleWindow  window;
    private volatile double     additiveIncrease       = 0.5;
    private volatile double     multiplicativeDecrease = 2;
    private final ReentrantLock lock                   = new ReentrantLock();

    public RateController(Predicate predicate) {
        this(predicate, 1000);
    }

    public RateController(Predicate predicate, double minimumRate,
                          double maximumRate, int windowSize) {
        this(predicate, windowSize);
        minimum = minimumRate;
        maximum = maximumRate;
    }

    public RateController(Predicate predicate, int windowSize) {
        window = new SampleWindow(windowSize);
        this.predicate = predicate;
    }

    @Override
    public boolean accept() {
        return predicate.accept();
    }

    public double getAdditiveIncrease() {
        return this.additiveIncrease;
    }

    public double getHighWaterMark() {
        return this.highWaterMark;
    }

    public double getLowWaterMark() {
        return this.lowWaterMark;
    }

    public double getMaximum() {
        return this.maximum;
    }

    @Override
    public double getMedianResponseTime() {
        return window.getMedian();
    }

    public double getMinimum() {
        return this.minimum;
    }

    public double getMultiplicativeDecrease() {
        return this.multiplicativeDecrease;
    }

    @Override
    public double getResponseTime() {
        return window.getPercentile(0.9);
    }

    public long getSampleRate() {
        return this.sampleRate;
    }

    public double getSmoothConstant() {
        return this.smoothConstant;
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
    public void sample(int rt) {
        ReentrantLock myLock = lock;
        if (!myLock.tryLock()) {
            // Skip sample if locked
            return;
        }
        try {
            long curtime = System.currentTimeMillis();

            if ((curtime - lastSampled) < sampleRate) {
                return;
            }
            window.sample(rt);
            lastSampled = curtime;
            double responseTime = window.getPercentile(0.9);

            if (responseTime < (lowWaterMark * target)) {
                increaseRate();
            } else if (responseTime > (highWaterMark * target)) {
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
    public void setTarget(double targetResponseTime) {
        target = targetResponseTime;
        predicate.setTargetRate(targetResponseTime);
    }

    protected void decreaseRate() {
        if (target > minimum) {
            target /= multiplicativeDecrease;
            if (target < minimum)
                target = minimum;
            predicate.setTargetRate(this.target);
        }
    }

    protected void increaseRate() {
        if (target < maximum) {
            target += additiveIncrease;
            if (target > maximum)
                target = maximum;
            predicate.setTargetRate(this.target);
        }
    }
}
