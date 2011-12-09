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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.LabeledThreadFactory;

/**
 * The java bean representation of the configuration of a Weaver.
 * 
 * @author hhildebrand
 * 
 */
public class WeaverConfigation {
    public static class RootDirectory {
        public final File directory;
        public final int  weight;

        public RootDirectory(File directory, int weight) {
            super();
            this.directory = directory;
            this.weight = weight;
        }
    }

    public static final long          DEFAULT_MAX_SEGMENTSIZE        = 1000 * 1024;
    public static final long          DEFAULT_PARTITION_TIMEOUT      = 60;
    public static final TimeUnit      DEFAULT_PARTITION_TIMEOUT_UNIT = TimeUnit.SECONDS;
    public static final int           DEFAULT_REPLICATION_QUEUE_SIZE = 100;

    public static final String        DEFAULT_STATE_NAME             = "weavers";

    private Node                      id;
    private long                      maxSegmentSize                 = DEFAULT_MAX_SEGMENTSIZE;
    private long                      partitionTimeout               = DEFAULT_PARTITION_TIMEOUT;
    private TimeUnit                  partitionTimeoutUnit           = DEFAULT_PARTITION_TIMEOUT_UNIT;
    private InetSocketAddress         replicationAddress             = new InetSocketAddress(
                                                                                             "127.0.0.1",
                                                                                             0);
    private int                       replicationQueueSize           = DEFAULT_REPLICATION_QUEUE_SIZE;
    private final SocketOptions       replicationSocketOptions       = new SocketOptions();
    private Executor                  replicators                    = Executors.newFixedThreadPool(10,
                                                                                                    new LabeledThreadFactory(
                                                                                                                             "replicator"));
    private final List<RootDirectory> roots                          = new ArrayList<RootDirectory>();
    private InetSocketAddress         spindleAddress                 = new InetSocketAddress(
                                                                                             "127.0.0.1",
                                                                                             0);
    private Executor                  spindles                       = Executors.newFixedThreadPool(3,
                                                                                                    new LabeledThreadFactory(
                                                                                                                             "spindle"));
    private final SocketOptions       spindleSocketOptions           = new SocketOptions();
    private String                    stateName                      = DEFAULT_STATE_NAME;

    public void addRoot(File directory) {
        addRoot(directory, 1);
    }

    public void addRoot(File directory, int weight) {
        roots.add(new RootDirectory(directory, weight));
    }

    /**
     * @return the id
     */
    public Node getId() {
        return id;
    }

    /**
     * @return the maxSegmentSize
     */
    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    /**
     * @return the partitionTimeout
     */
    public long getPartitionTimeout() {
        return partitionTimeout;
    }

    /**
     * @return the partitionTimeoutUnit
     */
    public TimeUnit getPartitionTimeoutUnit() {
        return partitionTimeoutUnit;
    }

    /**
     * @return the replicationAddress
     */
    public InetSocketAddress getReplicationAddress() {
        return replicationAddress;
    }

    /**
     * @return the replicationQueueSize
     */
    public int getReplicationQueueSize() {
        return replicationQueueSize;
    }

    /**
     * @return the replicationSocketOptions
     */
    public SocketOptions getReplicationSocketOptions() {
        return replicationSocketOptions;
    }

    /**
     * @return the replicators
     */
    public Executor getReplicators() {
        return replicators;
    }

    /**
     * @return the roots
     */
    public List<RootDirectory> getRoots() {
        return roots;
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
     * @return the stateName
     */
    public String getStateName() {
        return stateName;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(Node id) {
        this.id = id;
    }

    /**
     * @param maxSegmentSize
     *            the maxSegmentSize to set
     */
    public void setMaxSegmentSize(long maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    /**
     * @param partitionTimeout
     *            the partitionTimeout to set
     */
    public void setPartitionTimeout(long partitionTimeout) {
        this.partitionTimeout = partitionTimeout;
    }

    /**
     * @param partitionTimeoutUnit
     *            the partitionTimeoutUnit to set
     */
    public void setPartitionTimeoutUnit(TimeUnit partitionTimeoutUnit) {
        this.partitionTimeoutUnit = partitionTimeoutUnit;
    }

    /**
     * @param replicationAddress
     *            the replicationAddress to set
     */
    public void setReplicationAddress(InetSocketAddress replicationAddress) {
        this.replicationAddress = replicationAddress;
    }

    /**
     * @param replicationQueueSize
     *            the replicationQueueSize to set
     */
    public void setReplicationQueueSize(int replicationQueueSize) {
        this.replicationQueueSize = replicationQueueSize;
    }

    /**
     * @param replicators
     *            the replicators to set
     */
    public void setReplicators(Executor replicators) {
        this.replicators = replicators;
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

    /**
     * @param stateName
     *            the stateName to set
     */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    public void validate() {
        assert id != null : "Id must not be null";
        assert !roots.isEmpty() : "List of roots must not be empty";
    }
}