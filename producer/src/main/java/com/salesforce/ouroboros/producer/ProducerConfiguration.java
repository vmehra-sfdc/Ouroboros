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
package com.salesforce.ouroboros.producer;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ProducerConfiguration {
    private double maximumRate;
    private double minimumRate;
    private int    minRegenerationTime;
    private double targetRate;
    private int    tokenLimit;
    private int    windowSize;

    /**
     * @return the maximumRate
     */
    public double getMaximumRate() {
        return maximumRate;
    }

    /**
     * @return the minimumRate
     */
    public double getMinimumRate() {
        return minimumRate;
    }

    /**
     * @return the minRegenerationTime
     */
    public int getMinRegenerationTime() {
        return minRegenerationTime;
    }

    /**
     * @return the targetRate
     */
    public double getTargetRate() {
        return targetRate;
    }

    /**
     * @return the tokenLimit
     */
    public int getTokenLimit() {
        return tokenLimit;
    }

    /**
     * @return the windowSize
     */
    public int getWindowSize() {
        return windowSize;
    }

    /**
     * @param maximumRate
     *            the maximumRate to set
     */
    public void setMaximumRate(double maximumRate) {
        this.maximumRate = maximumRate;
    }

    /**
     * @param minimumRate
     *            the minimumRate to set
     */
    public void setMinimumRate(double minimumRate) {
        this.minimumRate = minimumRate;
    }

    /**
     * @param minRegenerationTime
     *            the minRegenerationTime to set
     */
    public void setMinRegenerationTime(int minRegenerationTime) {
        this.minRegenerationTime = minRegenerationTime;
    }

    /**
     * @param targetRate
     *            the targetRate to set
     */
    public void setTargetRate(double targetRate) {
        this.targetRate = targetRate;
    }

    /**
     * @param tokenLimit
     *            the tokenLimit to set
     */
    public void setTokenLimit(int tokenLimit) {
        this.tokenLimit = tokenLimit;
    }

    /**
     * @param windowSize
     *            the windowSize to set
     */
    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }
}
