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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.util.rate.Predicate;

/**
 * 
 * Input rate policing based on a token bucket.
 * 
 * @author hhildebrand
 * 
 */
public class RateLimiter implements Predicate {
    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    private double              currentTokens;
    private long                last;
    private int                 maxTokens;
    private final long          minimumRegenerationTime;
    private double              regenerationTime;

    /**
     * @param targetRate
     *            - the target rate limit for accepting new input
     * @param tokenLimit
     *            - the limit to the number of tokens in the bucket
     * @param minRegenerationTime
     *            - the minimum delay time, in Ms, to regenerate tokens
     */
    public RateLimiter(double targetRate, int tokenLimit,
                       int minRegenerationTime) {
        assert targetRate > 0;
        assert minRegenerationTime >= 0;
        assert tokenLimit > 0;
        minimumRegenerationTime = minRegenerationTime;
        regenerationTime = 1.0 / targetRate * 1.0e3;
        if (regenerationTime < 1) {
            regenerationTime = 1;
        }
        maxTokens = tokenLimit;
        currentTokens = tokenLimit * 1.0;
        last = -1;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.rate.Predicate#accept(int)
     */
    @Override
    public synchronized boolean accept(int cost, long currentTime) {
        long delay = last == -1L ? 0 : currentTime - last;

        if (delay >= minimumRegenerationTime) {
            // Regenerate tokens
            double numTokens = delay / regenerationTime;
            currentTokens += numTokens;
            if (currentTokens > maxTokens) {
                currentTokens = maxTokens;
            }
            last = currentTime;
        }

        if (currentTokens >= cost) {
            currentTokens -= cost;
            return true;
        } else {
            return false;
        }
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.rate.Predicate#accept()
     */
    @Override
    public boolean accept(long currentTime) {
        return accept(1, currentTime);
    }

    /**
     * @return the size of the token bucket
     */
    public int getCurrentTokens() {
        return (int) currentTokens;
    }

    /**
     * @return the current depth of the bucket
     */
    public int getMaxTokens() {
        return maxTokens;
    }

    public double getRegenerationTime() {
        return regenerationTime;
    }

    /**
     * Set the depth of the token bucket
     * 
     * @param depth
     */
    public synchronized void setMaxTokens(int depth) {
        maxTokens = depth;
    }

    @Override
    public synchronized void setTargetRate(double targetRate) {
        regenerationTime = 1.0 / targetRate * 1.0e3;
        if (regenerationTime < 1) {
            regenerationTime = 1;
        }
        if (log.isInfoEnabled()) {
            log.info(String.format("New regeneration time set to %s ms", regenerationTime));
        }
    }
}