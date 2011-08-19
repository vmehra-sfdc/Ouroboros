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
package com.salesforce.ouroboros.spindle;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import com.hellblazer.pinkie.SocketOptions;

/**
 * The java bean representation of the configuration of a Weaver.
 * 
 * @author hhildebrand
 * 
 */
public class WeaverConfigation {
    private long                maxSegmentSize;
    private InetSocketAddress   replicationAddress;
    private final SocketOptions replicationSocketOptions = new SocketOptions();
    private File                root;
    private InetSocketAddress   spindleAddress;
    private Executor            spindles;
    private final SocketOptions spindleSocketOptions     = new SocketOptions();

    /**
     * @return the maxSegmentSize
     */
    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    /**
     * @return the replicationAddress
     */
    public InetSocketAddress getReplicationAddress() {
        return replicationAddress;
    }

    /**
     * @return the replicationSocketOptions
     */
    public SocketOptions getReplicationSocketOptions() {
        return replicationSocketOptions;
    }

    /**
     * @return the root
     */
    public File getRoot() {
        return root;
    }

    /**
     * @return the spindleAddress
     */
    public InetSocketAddress getSpindleAddress() {
        return spindleAddress;
    }

    /**
     * @return the spindles
     */
    public Executor getSpindles() {
        return spindles;
    }

    /**
     * @return the spindleSocketOptions
     */
    public SocketOptions getSpindleSocketOptions() {
        return spindleSocketOptions;
    }

    /**
     * @param maxSegmentSize
     *            the maxSegmentSize to set
     */
    public void setMaxSegmentSize(long maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    /**
     * @param replicationAddress
     *            the replicationAddress to set
     */
    public void setReplicationAddress(InetSocketAddress replicationAddress) {
        this.replicationAddress = replicationAddress;
    }

    /**
     * @param root
     *            the root to set
     */
    public void setRoot(File root) {
        this.root = root;
    }

    /**
     * @param spindleAddress
     *            the spindleAddress to set
     */
    public void setSpindleAddress(InetSocketAddress spindleAddress) {
        this.spindleAddress = spindleAddress;
    }

    /**
     * @param spindles
     *            the spindles to set
     */
    public void setSpindles(Executor spindles) {
        this.spindles = spindles;
    }
}