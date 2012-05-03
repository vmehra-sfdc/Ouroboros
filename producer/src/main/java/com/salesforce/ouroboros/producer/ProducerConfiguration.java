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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.DefaultSkipStrategy;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy;
import com.salesforce.ouroboros.util.LabeledThreadFactory;

/**
 * 
 * @author hhildebrand
 * 
 */
public class ProducerConfiguration {
    private double              maximumBandwidth             = 20000000.0;
    private int                 maxQueueLength               = 100;
    private double              minimumBandwidth             = 500000.0;
    private int                 minimumTokenRegenerationTime = 1;
    private int                 numberOfReplicas             = 200;
    private int                 retryLimit                   = 10;
    private int                 sampleFrequency              = 10;
    private int                 sampleWindowSize             = 1000;
    private SkipStrategy<Node>  skipStrategy                 = new DefaultSkipStrategy();
    private ExecutorService     spinners                     = Executors.newFixedThreadPool(5,
                                                                                            new LabeledThreadFactory(
                                                                                                                     "Spinner"));
    private final SocketOptions spinnerSocketOptions         = new SocketOptions();
    private double              targetBandwidth              = 1000000.0;
    private int                 tokenLimit                   = 2000000;
    private double              targetPercentile             = 0.9;

    /**
     * @return the maximumBandwidth
     */
    public double getMaximumBandwidth() {
        return maximumBandwidth;
    }

    public int getMaxQueueLength() {
        return maxQueueLength;
    }

    /**
     * @return the minimumEventRate
     */
    public double getMinimumBandwidth() {
        return minimumBandwidth;
    }

    /**
     * @return the minimumTokenRegenerationTime
     */
    public int getMinimumTokenRegenerationTime() {
        return minimumTokenRegenerationTime;
    }

    /**
     * @return the numberOfReplicas
     */
    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    /**
     * @return the sampleFrequency
     */
    public int getSampleFrequency() {
        return sampleFrequency;
    }

    /**
     * @return the sampleWindowSize
     */
    public int getSampleWindowSize() {
        return sampleWindowSize;
    }

    /**
     * @return the skipStrategy
     */
    public SkipStrategy<Node> getSkipStrategy() {
        return skipStrategy;
    }

    /**
     * @return the spinners
     */
    public ExecutorService getSpinners() {
        return spinners;
    }

    /**
     * @return the spinnerSocketOptions
     */
    public SocketOptions getSpinnerSocketOptions() {
        return spinnerSocketOptions;
    }

    /**
     * @return the targetBandwidth
     */
    public double getTargetBandwidth() {
        return targetBandwidth;
    }

    /**
     * @return the targetPercentile
     */
    public double getTargetPercentile() {
        return targetPercentile;
    }

    /**
     * @return the tokenLimit
     */
    public int getTokenLimit() {
        return tokenLimit;
    }

    /**
     * @param maximumEventRate
     *            the maximumEventRate to set
     */
    public void setMaximumBandwidth(double maximumEventRate) {
        maximumBandwidth = maximumEventRate;
    }

    public void setMaxQueueLength(int maxQueueLength) {
        this.maxQueueLength = maxQueueLength;
    }

    /**
     * @param minimumEventRate
     *            the minimumEventRate to set
     */
    public void setMinimumBandwidth(double minimumEventRate) {
        minimumBandwidth = minimumEventRate;
    }

    /**
     * @param minimumTokenRegenerationTime
     *            the minimumTokenRegenerationTime to set
     */
    public void setMinimumTokenRegenerationTime(int minimumTokenRegenerationTime) {
        this.minimumTokenRegenerationTime = minimumTokenRegenerationTime;
    }

    /**
     * @param numberOfReplicas
     *            the numberOfReplicas to set
     */
    public void setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    public void setRetryLimit(int resendLimit) {
        retryLimit = resendLimit;
    }

    /**
     * @param sampleFrequency
     *            the sampleFrequency to set
     */
    public void setSampleFrequency(int sampleFrequency) {
        this.sampleFrequency = sampleFrequency;
    }

    /**
     * @param sampleWindowSize
     *            the sampleWindowSize to set
     */
    public void setSampleWindowSize(int sampleWindowSize) {
        this.sampleWindowSize = sampleWindowSize;
    }

    /**
     * @param skipStrategy
     *            the skipStrategy to set
     */
    public void setSkipStrategy(SkipStrategy<Node> skipStrategy) {
        this.skipStrategy = skipStrategy;
    }

    /**
     * @param spinners
     *            the spinners to set
     */
    public void setSpinners(ExecutorService spinners) {
        this.spinners = spinners;
    }

    /**
     * @param targetBandwidth
     *            the targetBandwidth to set
     */
    public void setTargetBandwidth(double targetBandwidth) {
        this.targetBandwidth = targetBandwidth;
    }

    /**
     * @param targetPercentile
     *            the targetPercentile to set
     */
    public void setTargetPercentile(double targetPercentile) {
        this.targetPercentile = targetPercentile;
    }

    /**
     * @param tokenLimit
     *            the tokenLimit to set
     */
    public void setTokenLimit(int tokenLimit) {
        this.tokenLimit = tokenLimit;
    }
}
