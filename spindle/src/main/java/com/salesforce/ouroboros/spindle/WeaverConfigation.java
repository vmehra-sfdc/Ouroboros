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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.DefaultSkipStrategy;
import com.salesforce.ouroboros.NoSkipStrategy;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy;
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
    private static final String       REPLICATOR                     = "replicator";
    private static final String       SPINDLE                        = "spindle";
    private static final String       XEROX                          = "xerox";

    private int                       appendSegmentConcurrencyLevel  = 16;
    private Node                      id;
    private int                       initialAppendSegmentCapacity   = 16;
    private int                       initialReadSegmentCapacity     = 16;
    private int                       maximumAppendSegmentCapacity   = 4096;
    private int                       maximumReadSegmentCapacity     = 4096;
    private long                      maxSegmentSize                 = DEFAULT_MAX_SEGMENTSIZE;
    private int                       numberOfReplicas               = 200;
    private int                       numberOfRootReplicas           = 200;
    private long                      partitionTimeout               = DEFAULT_PARTITION_TIMEOUT;
    private TimeUnit                  partitionTimeoutUnit           = DEFAULT_PARTITION_TIMEOUT_UNIT;
    private int                       readSegmentConcurrencyLevel    = 16;
    private InetSocketAddress         replicationAddress             = new InetSocketAddress(
                                                                                             "127.0.0.1",
                                                                                             0);
    private int                       replicationQueueSize           = DEFAULT_REPLICATION_QUEUE_SIZE;
    private final SocketOptions       replicationSocketOptions       = new SocketOptions();
    private ExecutorService           replicators                    = Executors.newCachedThreadPool(new LabeledThreadFactory(
                                                                                                                              REPLICATOR));
    private final List<RootDirectory> roots                          = new ArrayList<RootDirectory>();
    private SkipStrategy<File>        rootSkipStrategy               = new NoSkipStrategy<File>();
    private SkipStrategy<Node>        skipStrategy                   = new DefaultSkipStrategy();
    private InetSocketAddress         spindleAddress                 = new InetSocketAddress(
                                                                                             "127.0.0.1",
                                                                                             0);

    private ExecutorService           spindles                       = Executors.newCachedThreadPool(new LabeledThreadFactory(
                                                                                                                              SPINDLE));
    private final SocketOptions       spindleSocketOptions           = new SocketOptions();
    private String                    stateName                      = DEFAULT_STATE_NAME;
    private InetSocketAddress         xeroxAddress                   = new InetSocketAddress(
                                                                                             "127.0.0.1",
                                                                                             0);
    private ExecutorService           xeroxes                        = Executors.newCachedThreadPool(new LabeledThreadFactory(
                                                                                                                              XEROX));
    private final SocketOptions       xeroxSocketOptions             = new SocketOptions();

    public void addRoot(File directory) {
        addRoot(directory, 1);
    }

    public void addRoot(File directory, int weight) {
        roots.add(new RootDirectory(directory, weight));
    }

    /**
     * @return the appendSegmentConcurrencyLevel
     */
    public int getAppendSegmentConcurrencyLevel() {
        return appendSegmentConcurrencyLevel;
    }

    /**
     * @return the id
     */
    public Node getId() {
        return id;
    }

    /**
     * @return the initialAppendSegmentCapacity
     */
    public int getInitialAppendSegmentCapacity() {
        return initialAppendSegmentCapacity;
    }

    /**
     * @return the initialReadSegmentCapacity
     */
    public int getInitialReadSegmentCapacity() {
        return initialReadSegmentCapacity;
    }

    /**
     * @return the maximumAppendSegmentCapacity
     */
    public int getMaximumAppendSegmentCapacity() {
        return maximumAppendSegmentCapacity;
    }

    /**
     * @return the maximumReadSegmentCapacity
     */
    public int getMaximumReadSegmentCapacity() {
        return maximumReadSegmentCapacity;
    }

    /**
     * @return the maxSegmentSize
     */
    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    /**
     * @return the numberOfReplicas
     */
    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    /**
     * @return the numberOfRootReplicas
     */
    public int getNumberOfRootReplicas() {
        return numberOfRootReplicas;
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
     * @return the readSegmentConcurrencyLevel
     */
    public int getReadSegmentConcurrencyLevel() {
        return readSegmentConcurrencyLevel;
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
    public ExecutorService getReplicators() {
        return replicators;
    }

    /**
     * @return the roots
     */
    public List<RootDirectory> getRoots() {
        return roots;
    }

    /**
     * @return the rootSkipStrategy
     */
    public SkipStrategy<File> getRootSkipStrategy() {
        return rootSkipStrategy;
    }

    /**
     * @return the skipStrategy
     */
    public SkipStrategy<Node> getSkipStrategy() {
        return skipStrategy;
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
    public ExecutorService getSpindles() {
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
     * @return the xeroxAddress
     */
    public InetSocketAddress getXeroxAddress() {
        return xeroxAddress;
    }

    /**
     * @return the xeroxes
     */
    public ExecutorService getXeroxes() {
        return xeroxes;
    }

    /**
     * @return the xeroxSocketOptions
     */
    public SocketOptions getXeroxSocketOptions() {
        return xeroxSocketOptions;
    }

    /**
     * @param segmentConcurrencyLevel
     *            the segmentConcurrencyLevel to set
     */
    public void setAppendSegmentConcurrencyLevel(int segmentConcurrencyLevel) {
        appendSegmentConcurrencyLevel = segmentConcurrencyLevel;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(Node id) {
        this.id = id;
    }

    /**
     * @param initialSegmentCapacity
     *            the initialSegmentCapacity to set
     */
    public void setInitialAppendSegmentCapacity(int initialSegmentCapacity) {
        initialAppendSegmentCapacity = initialSegmentCapacity;
    }

    /**
     * @param initialReadSegmentCapacity
     *            the initialReadSegmentCapacity to set
     */
    public void setInitialReadSegmentCapacity(int initialReadSegmentCapacity) {
        this.initialReadSegmentCapacity = initialReadSegmentCapacity;
    }

    /**
     * @param maximumSegmentCapacity
     *            the maximumSegmentCapacity to set
     */
    public void setMaximumAppendSegmentCapacity(int maximumSegmentCapacity) {
        maximumAppendSegmentCapacity = maximumSegmentCapacity;
    }

    /**
     * @param maximumReadSegmentCapacity
     *            the maximumReadSegmentCapacity to set
     */
    public void setMaximumReadSegmentCapacity(int maximumReadSegmentCapacity) {
        this.maximumReadSegmentCapacity = maximumReadSegmentCapacity;
    }

    /**
     * @param maxSegmentSize
     *            the maxSegmentSize to set
     */
    public void setMaxSegmentSize(long maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    /**
     * @param numberOfReplicas
     *            the numberOfReplicas to set
     */
    public void setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    /**
     * @param numberOfRootReplicas
     *            the numberOfRootReplicas to set
     */
    public void setNumberOfRootReplicas(int numberOfRootReplicas) {
        this.numberOfRootReplicas = numberOfRootReplicas;
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
     * @param readSegmentConcurrencyLevel
     *            the readSegmentConcurrencyLevel to set
     */
    public void setReadSegmentConcurrencyLevel(int readSegmentConcurrencyLevel) {
        this.readSegmentConcurrencyLevel = readSegmentConcurrencyLevel;
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
    public void setReplicators(ExecutorService replicators) {
        this.replicators = replicators;
    }

    /**
     * @param rootSkipStrategy
     *            the rootSkipStrategy to set
     */
    public void setRootSkipStrategy(SkipStrategy<File> rootSkipStrategy) {
        this.rootSkipStrategy = rootSkipStrategy;
    }

    /**
     * @param skipStrategy
     *            the skipStrategy to set
     */
    public void setSkipStrategy(SkipStrategy<Node> skipStrategy) {
        this.skipStrategy = skipStrategy;
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
    public void setSpindles(ExecutorService spindles) {
        this.spindles = spindles;
    }

    /**
     * @param stateName
     *            the stateName to set
     */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    /**
     * @param xeroxAddress
     *            the xeroxAddress to set
     */
    public void setXeroxAddress(InetSocketAddress xeroxAddress) {
        this.xeroxAddress = xeroxAddress;
    }

    /**
     * @param xeroxes
     *            the xeroxes to set
     */
    public void setXeroxes(ExecutorService xeroxes) {
        this.xeroxes = xeroxes;
    }

    public void validate() {
        assert id != null : "Id must not be null";
        assert !roots.isEmpty() : "List of roots must not be empty";
    }
}