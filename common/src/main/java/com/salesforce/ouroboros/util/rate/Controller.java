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
package com.salesforce.ouroboros.util.rate;

/**
 * 
 * @author hhildebrand
 * 
 */
public interface Controller {

    /**
     * @return true if a new element can be accepted, using the default cost
     */
    public abstract boolean accept();

    /**
     * @return true if a new element can be accepted, using the supplied cost
     */
    public abstract boolean accept(int cost);

    /**
     * Answer the target response time
     * 
     * @return
     */
    public double getTarget();

    /**
     * notify the controller of a new sample
     * 
     * @param responseTime
     *            - the new sample
     */
    public void sample(int responseTime);

    /**
     * @return the median of the sampled response time over the history of the
     *         controller
     */
    double getMedianResponseTime();

    /**
     * @return the 90th percentile of the sampled response time over the history
     *         of the controller
     */
    double getResponseTime();

    /**
     * Answer the size of the sample window
     * 
     * @return the size of the sample window
     */
    int getWindow();

    /**
     * Set the target response time, in milliseconds
     * 
     * @param target
     */
    void setTarget(double target);

}