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

import static com.salesforce.ouroboros.util.Utils.point;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.hellblazer.pinkie.ChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.api.producer.UnknownChannelException;
import com.salesforce.ouroboros.producer.spinner.Batch;
import com.salesforce.ouroboros.producer.spinner.Spinner;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Gate;
import com.salesforce.ouroboros.util.rate.Controller;
import com.salesforce.ouroboros.util.rate.controllers.RateController;
import com.salesforce.ouroboros.util.rate.controllers.RateLimiter;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Producer {

    public static class UpdateState implements Serializable {
        private static final long serialVersionUID = 1L;

        public final UUID         channel;
        public final long         timestamp;

        public UpdateState(UUID channel, long timestamp) {
            super();
            this.channel = channel;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", channel, timestamp);
        }
    }

    private static class ChannelState {
        volatile Spinner spinner;
        volatile long    timestamp;

        public ChannelState(Spinner spinner, long timestamp) {
            this.spinner = spinner;
            this.timestamp = timestamp;
        }
    }

    private static final Logger                     log               = Logger.getLogger(Producer.class.getCanonicalName());

    private final ConcurrentMap<UUID, ChannelState> channelState      = new ConcurrentHashMap<UUID, ChannelState>();
    private final Controller                        controller;
    private final int                               maxQueueLength;
    private final Map<UUID, Long>                   mirrors           = new ConcurrentHashMap<UUID, Long>();
    private ConsistentHashFunction<Node>            producerRing      = new ConsistentHashFunction<Node>();
    private final Gate                              publishGate       = new Gate();
    private final AtomicInteger                     publishingThreads = new AtomicInteger(
                                                                                          0);
    private final int                               retryLimit;
    private final Node                              self;
    private final EventSource                       source;
    private final ChannelHandler                    spinnerHandler;
    private final ConcurrentMap<Node, Spinner>      spinners          = new ConcurrentHashMap<Node, Spinner>();
    private ConsistentHashFunction<Node>            weaverRing        = new ConsistentHashFunction<Node>();
    private ConsistentHashFunction<Node>            nextProducerRing;

    public Producer(Node self, EventSource source,
                    ProducerConfiguration configuration) throws IOException {
        controller = new RateController(
                                        new RateLimiter(
                                                        configuration.getTargetEventRate(),
                                                        configuration.getTokenLimit(),
                                                        configuration.getMinimumTokenRegenerationTime()),
                                        configuration.getMinimumEventRate(),
                                        configuration.getMaximumEventRate(),
                                        configuration.getSampleWindowSize(),
                                        configuration.getSampleFrequency());
        this.self = self;
        this.source = source;
        spinnerHandler = new ChannelHandler(
                                            "Spinner Handler",
                                            configuration.getSpinnerSocketOptions(),
                                            configuration.getSpinners());
        maxQueueLength = configuration.getMaxQueueLength();
        retryLimit = configuration.getRetryLimit();
        openPublishingGate();
    }

    /**
     * Acknowledge the commited event batch, where this node is the primary
     * event producer for the channel of this batch
     * 
     * @param batch
     *            - The committed event batch
     */
    public void acknowledge(Batch batch) {
        controller.sample(batch.interval());
        channelState.get(batch.channel).timestamp = batch.timestamp;
    }

    /**
     * Acknowledge the commited event batch, where this node is the mirror event
     * producer for the channel for this batch
     * 
     * @param ack
     *            - the identity of the batch
     */
    public void acknowledge(BatchIdentity ack) {
        mirrors.put(ack.channel, ack.timestamp);
    }

    public void closed(UUID channel) {
        ChannelState state = channelState.remove(channel);
        if (state != null) {
            state.spinner.close(channel);
        }
        source.closed(channel);
    }

    public Node getId() {
        return self;
    }

    /**
     * The channel has been opened.
     * 
     * @param channel
     *            - the id of the channel
     */
    public void opened(UUID channel) {
        Node[] pair = getProducerReplicationPair(channel);
        if (self.equals(pair[1])) {
            mirrors.put(channel, 0L);
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Channel %s opened on mirror %s",
                                       channel, this));
            }
        } else {
            mapSpinner(channel);
            source.opened(channel);
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Channel %s opened on primary %s",
                                       channel, this));
            }
        }
    }

    /**
     * Publish the events on the indicated channel
     * 
     * @param channel
     *            - the id of the event channel
     * @param timestamp
     *            - the timestamp for this batch
     * @param events
     *            - the batch of events to publish to the channel
     * 
     * @throws RateLimiteExceededException
     *             - if the event publishing rate has been exceeded
     * @throws UnknownChannelException
     *             - if the supplied channel id does not exist
     * @throws InterruptedException
     *             - if the thread is interrupted
     */
    public void publish(UUID channel, long timestamp,
                        Collection<ByteBuffer> events)
                                                      throws RateLimiteExceededException,
                                                      UnknownChannelException,
                                                      InterruptedException {
        publishGate.await();
        publishingThreads.incrementAndGet();
        try {
            ChannelState state = channelState.get(channel);
            if (state == null) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Push to a channel that does not exist: %s",
                                           channel));
                }
                throw new UnknownChannelException(
                                                  String.format("The channel %s does not exist",
                                                                channel));
            }
            if (!controller.accept(events.size())) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Rate limit exceeded for push to %s",
                                           channel));
                }
                throw new RateLimiteExceededException(
                                                      String.format("The rate limit for this producer has been exceeded"));
            }
            state.spinner.push(new Batch(
                                         getProducerReplicationPair(channel)[1],
                                         channel, timestamp, events));
        } finally {
            publishingThreads.decrementAndGet();
        }
    }

    /**
     * Answer the remapped primary/mirror pairs using the nextRing
     * 
     * @param nextRing
     *            - the new weaver hash ring
     * @return the remapping of channels hosted on this node that have changed
     *         their primary or mirror in the new hash ring
     */
    protected Map<UUID, Node[][]> remap() {
        Map<UUID, Node[][]> remapped = new HashMap<UUID, Node[][]>();
        for (UUID channel : channelState.keySet()) {
            long channelPoint = point(channel);
            List<Node> newPair = nextProducerRing.hash(channelPoint, 2);
            List<Node> oldPair = producerRing.hash(channelPoint, 2);
            if (oldPair.contains(self)) {
                if (!oldPair.get(0).equals(newPair.get(0))
                    || !oldPair.get(1).equals(newPair.get(1))) {
                    remapped.put(channel,
                                 new Node[][] {
                                         new Node[] { oldPair.get(0),
                                                 oldPair.get(1) },
                                         new Node[] { newPair.get(0),
                                                 newPair.get(1) } });
                }
            }
        }
        for (UUID channel : mirrors.keySet()) {
            long channelPoint = point(channel);
            List<Node> newPair = nextProducerRing.hash(channelPoint, 2);
            List<Node> oldPair = producerRing.hash(channelPoint, 2);
            if (oldPair.contains(self)) {
                if (!oldPair.get(0).equals(newPair.get(0))
                    || !oldPair.get(1).equals(newPair.get(1))) {
                    remapped.put(channel,
                                 new Node[][] {
                                         new Node[] { oldPair.get(0),
                                                 oldPair.get(1) },
                                         new Node[] { newPair.get(0),
                                                 newPair.get(1) } });
                }
            }
        }
        return remapped;
    }

    /**
     * Remap the spinners by incorporating the set of new weavers into the
     * consistent hash ring. This amounts to doing the equivalent of a failover
     * to the newly mapped primaries: all currently pending pushes for the old
     * primaries will be reenqueued on the new primaries.
     * 
     * @param nextWeaverRing
     *            - the new weaver hash ring
     */
    public void remapWeavers(ConsistentHashFunction<Node> nextWeaverRing) {
        for (Entry<UUID, ChannelState> mapping : channelState.entrySet()) {
            Spinner remapped = spinners.get(nextWeaverRing.hash(point(mapping.getKey())));
            if (remapped != mapping.getValue().spinner) {
                mapping.getValue().spinner = remapped;
                for (Batch batch : mapping.getValue().spinner.getPending(mapping.getKey()).values()) {
                    boolean delivered = false;
                    for (int i = 0; i < retryLimit; i++) {
                        try {
                            remapped.push(batch);
                            delivered = true;
                            break;
                        } catch (RateLimiteExceededException e) {
                            int sleepMs = 100 * i;
                            if (log.isLoggable(Level.INFO)) {
                                log.info(String.format("Rate limit exceeded sending queued events to new primary for %s, sleeping %s milliseconds on %s",
                                                       mapping.getKey(),
                                                       sleepMs, self));
                            }
                            try {
                                Thread.sleep(sleepMs);
                            } catch (InterruptedException e1) {
                                return;
                            }
                        }
                    }
                    if (!delivered) {
                        log.severe(String.format("Unable to remap queued events on %s for %s due to rate limiting",
                                                 self, mapping.getKey()));
                    }
                }
            }
        }
        weaverRing = nextWeaverRing;
    }

    /**
     * Start the coordinator
     */
    @PostConstruct
    public void start() {
        spinnerHandler.start();
    }

    /**
     * Terminate the coordinator
     */
    @PreDestroy
    public void terminate() {
        spinnerHandler.terminate();
    }

    /**
     * Perform the failover for this node. Failover to the channel mirrors and
     * assume primary producer responsibility for channels this node was
     * mirroring
     * 
     * @param deadMembers
     *            - the deceased
     * @throws InterruptedException
     *             - if the thread is interrupted
     */
    public void failover(Collection<Node> deadMembers)
                                                      throws InterruptedException {
        // Coordinate the failover with the publishing threads
        closePublishingGate();

        // Initiate the failover procedure for the dead spinners
        for (Entry<Node, Spinner> entry : spinners.entrySet()) {
            if (deadMembers.contains(entry.getKey())) {
                entry.getValue().failover();
            }
        }

        // Failover mirror channels for which this node is now the primary
        Map<UUID, Long> newPrimaries = new HashMap<UUID, Long>();
        for (Iterator<Entry<UUID, Long>> mirrored = mirrors.entrySet().iterator(); mirrored.hasNext();) {
            Entry<UUID, Long> entry = mirrored.next();
            UUID channel = entry.getKey();
            Node[] producerPair = getProducerReplicationPair(channel);
            if (deadMembers.contains(producerPair[0])) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("%s is assuming primary role for channel %s from Producer[%s]",
                                           this, channel,
                                           producerPair[0].processId));
                }
                newPrimaries.put(channel, entry.getValue());
                mirrored.remove();
                ChannelState previous = mapSpinner(channel);
                assert previous == null : String.format("Apparently node %s is already primary for %s");
            }
        }

        // Assign failover mirror for dead primaries
        for (Entry<UUID, ChannelState> entry : channelState.entrySet()) {
            Node[] pair = getChannelBufferReplicationPair(entry.getKey());
            if (deadMembers.contains(pair[0])) {
                Spinner newPrimary = spinners.get(entry.getKey());
                if (newPrimary == null) {
                    if (log.isLoggable(Level.WARNING)) {
                        log.warning(String.format("Both the primary and the secondary for %s have failed!",
                                                  entry.getKey()));
                    }
                } else {
                    ChannelState failedPrimary = entry.getValue();
                    // Fail over to the new primary
                    for (Batch batch : failedPrimary.spinner.getPending(entry.getKey()).values()) {
                        boolean delivered = false;
                        for (int i = 0; i < retryLimit; i++) {
                            try {
                                newPrimary.push(batch);
                                delivered = true;
                                break;
                            } catch (RateLimiteExceededException e) {
                                int sleepMs = 100 * i;
                                if (log.isLoggable(Level.INFO)) {
                                    log.info(String.format("Rate limit exceeded sending queued events to new primary for %s, sleeping %s milliseconds on %s",
                                                           entry.getKey(),
                                                           sleepMs, self));
                                }
                                Thread.sleep(sleepMs);
                            }
                        }
                        if (!delivered) {
                            log.severe(String.format("Unable to failover queued batch %s on %s for %s due to rate limiting",
                                                     batch, self,
                                                     entry.getKey()));
                        }
                    }
                    failedPrimary.spinner = newPrimary;
                }
            }
        }

        // Remove dead spinners
        for (Iterator<Entry<Node, Spinner>> entries = spinners.entrySet().iterator(); entries.hasNext();) {
            Entry<Node, Spinner> entry = entries.next();
            if (deadMembers.contains(entry.getKey())) {
                entry.getValue().close();
                entries.remove();
            }
        }

        // Let any publishing threads precede
        openPublishingGate();

        if (newPrimaries.size() != 0) {
            source.assumePrimary(newPrimaries);
        }
    }

    private ChannelState mapSpinner(UUID channel) {
        Node[] channelPair = getChannelBufferReplicationPair(channel);
        Spinner spinner = spinners.get(channelPair[0]);
        if (spinner == null) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Primary node %s for channel %s is down, mapping mirror %s on %s",
                                       channelPair[0], channel, channelPair[1],
                                       this));
            }
            spinner = spinners.get(channelPair[1]);
            if (spinner == null) {
                if (log.isLoggable(Level.SEVERE)) {
                    log.severe(String.format("Mirror %s for channel %s is down on %s",
                                             channelPair[0], channel, this));
                }
                return null;
            } else {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Mapping channel %s to mirror %s on %s",
                                           channel, channelPair[1], this));
                }
                return channelState.putIfAbsent(channel,
                                                new ChannelState(spinner, -1));
            }
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Mapping channel %s to primary %s on %s",
                                       channel, channelPair[0], this));
            }
            return channelState.putIfAbsent(channel, new ChannelState(spinner,
                                                                      -1));
        }
    }

    protected void closePublishingGate() throws InterruptedException {
        publishGate.close();
        // Wait until all publishing threads are finished
        while (publishingThreads.get() != 0) {
            Thread.sleep(1);
        }
    }

    /**
     * Create the spinners to the new set of weavers in the partition.
     */
    protected void createSpinners(Collection<Node> weavers,
                                  Map<Node, ContactInformation> yellowPages) {
        for (Node n : weavers) {
            ContactInformation info = yellowPages.get(n);
            assert info != null : String.format("Did not find any connection information for node %s",
                                                n);
            Spinner spinner = new Spinner(this, n, maxQueueLength);
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Connecting spinner to %s on %s address %s",
                                       n, self, info.spindle));
            }
            try {
                spinnerHandler.connectTo(info.spindle, spinner);
            } catch (IOException e) {
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            String.format("Cannot connect to spindle on node %s at address %s",
                                          n, info.spindle), e);
                }
            }
            spinners.put(n, spinner);
        }
    }

    /**
     * Answer the channel buffer process pair responsible for the channel
     * 
     * @param channel
     *            - the id of the channel
     * @return the array of length 2, containing the primary and mirror Node
     *         responsible for the channel
     */
    protected Node[] getChannelBufferReplicationPair(UUID channel) {
        assert weaverRing.size() > 1 : String.format("Insufficient weavers in the hashring: %s",
                                                     weaverRing.size());
        List<Node> pair = weaverRing.hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    /**
     * Answer the producer process pair responsible for the channel
     * 
     * @param channel
     *            - the id of the channel
     * @return the array of length 2, containing the primary and mirror Node
     *         responsible for the channel
     */
    protected Node[] getProducerReplicationPair(UUID channel) {
        assert producerRing.size() > 1 : String.format("Insufficient producers in the hashring: %s",
                                                       producerRing.size());
        List<Node> pair = producerRing.hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    protected void openPublishingGate() {
        publishGate.open();
    }

    protected void setProducerRing(ConsistentHashFunction<Node> newRing) {
        producerRing = newRing;
    }

    /**
     * 
     * @param channel
     * @return true if this producer is the primary or mirror for the channel
     */
    public boolean isResponsibleFor(UUID channel) {
        Node[] pair = getProducerReplicationPair(channel);
        return self.equals(pair[0]) || self.equals(pair[1]);
    }

    /**
     * 
     * @param channel
     * @return true if this producer is the primary for the channel
     */
    public boolean isPrimaryFor(UUID channel) {
        return self.equals(getProducerReplicationPair(channel)[0]);
    }

    /**
     * 
     * @param channel
     * @return true if this producer is the mirror for the channel
     */
    public boolean isMirrorFor(UUID channel) {
        return self.equals(getProducerReplicationPair(channel)[0]);
    }

    /**
     * 
     * @param channel
     * @return true if this producer is the mirror for the channel
     */
    public boolean isActingMirrorFor(UUID channel) {
        return mirrors.containsKey(channel);
    }

    /**
     * 
     * @param channel
     * @return true if this producer is the primary for the channel
     */
    public boolean isActingPrimaryFor(UUID channel) {
        return channelState.containsKey(channel);
    }

    public String toString() {
        return String.format("Producer[%s]", self.processId);
    }

    public void setNextProducerRing(ConsistentHashFunction<Node> newRing) {
        nextProducerRing = newRing;
    }

    /**
     * Commit the producer ring and open the channels we're now the primary for.
     * 
     * @param rebalanceUpdates
     */
    public void commitProducerRing(List<UpdateState> rebalanceUpdates) {
        producerRing = nextProducerRing;
        for (Iterator<Entry<UUID, ChannelState>> mappings = channelState.entrySet().iterator(); mappings.hasNext();) {
            Entry<UUID, ChannelState> mapping = mappings.next();
            Node primary = nextProducerRing.hash(point(mapping.getKey()));
            if (!primary.equals(self)) {
                mappings.remove();
            }
        }
        for (Iterator<Entry<UUID, Long>> mappings = mirrors.entrySet().iterator(); mappings.hasNext();) {
            Entry<UUID, Long> mapping = mappings.next();
            List<Node> pair = nextProducerRing.hash(point(mapping.getKey()), 2);
            if (!pair.get(1).equals(self)) {
                mappings.remove();
            }
        }
        for (UpdateState update : rebalanceUpdates) {
            if (isPrimaryFor(update.channel)) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("%s assuming primary for %s", self,
                                           update));
                }
                mapSpinner(update.channel);
            } else {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("%s assuming mirror for %s", self,
                                           update));
                }
                mirrors.put(update.channel, update.timestamp);
            }
        }
        nextProducerRing = null;
    }

    /**
     * Rebalance a channel that this node serves as either the primary or
     * mirror. Add the list of channels to the map of update states that will
     * perform the state transfer, if needed, between this node and other nodes
     * also responsible for the channel
     * 
     * @param remapped
     *            - the map of Nodes to the list of updates for that node
     * 
     * @param channel
     *            - the id of the channel to rebalance
     */
    public void rebalance(HashMap<Node, List<UpdateState>> remapped,
                          UUID channel, Node originalPrimary,
                          Node originalMirror, Node remappedPrimary,
                          Node remappedMirror, Collection<Node> deadMembers) {
        ChannelState state = channelState.get(channel);
        long timestamp;
        if (state == null) {
            Long ts = mirrors.get(channel);
            assert ts != null : String.format("The event channel  %s to rebalance does not exist on %s",
                                              channel, self);
            timestamp = ts;
        } else {
            timestamp = state.timestamp;
        }
        if (self.equals(originalPrimary)) {
            // self is the primary
            if (deadMembers.contains(originalMirror)) {
                // mirror is down
                if (self.equals(remappedPrimary)) {
                    // if self is still the primary
                    // Xerox state to the new mirror
                    infoLog("Rebalancing for %s from primary %s to new mirror %s",
                            channel, self, remappedMirror);
                    remap(channel, timestamp, remappedMirror, remapped);
                } else if (self.equals(remappedMirror)) {
                    // Self becomes the new mirror
                    infoLog("Rebalancing for %s, %s becoming mirror from primary, new primary %s",
                            channel, self);
                    remap(channel, timestamp, self, remapped);
                    remap(channel, timestamp, remappedPrimary, remapped);
                } else {
                    // Xerox state to new primary and mirror
                    infoLog("Rebalancing for %s from old primary %s to new primary %s, new mirror %s",
                            channel, self, remappedPrimary, remappedMirror);
                    remap(channel, timestamp, remappedPrimary, remapped);
                    remap(channel, timestamp, remappedMirror, remapped);
                }
            } else if (!self.equals(remappedPrimary)) {
                // mirror is up
                // Xerox state to the new primary
                infoLog("Rebalancing for %s from old primary %s to new primary %s",
                        channel, self, remappedPrimary);
                remap(channel, timestamp, remappedPrimary, remapped);
                if (self.equals(remappedMirror)) {
                    remap(channel, timestamp, self, remapped);
                }
            }
        } else if (deadMembers.contains(originalPrimary)) {
            assert self.equals(originalMirror);
            // self is the secondary
            // primary is down
            if (self.equals(remappedMirror)) {
                // Self is still the mirror
                // Xerox state to the new primary
                infoLog("Rebalancing for %s from mirror %s to new primary %s",
                        channel, self, remappedPrimary);
                remap(channel, timestamp, remappedPrimary, remapped);
            } else if (self.equals(remappedPrimary)) {
                // Self becomes the new primary
                infoLog("Rebalancing for %s, %s becoming primary from mirror, new mirror %s",
                        channel, self, remappedMirror);
                remap(channel, timestamp, self, remapped);
                remap(channel, timestamp, remappedMirror, remapped);
            } else {
                // Xerox state to the new primary and mirror
                infoLog("Rebalancing for %s from old mirror %s to new primary %s, new mirror %s",
                        channel, self, remappedPrimary, remappedMirror);
                remap(channel, timestamp, remappedPrimary, remapped);
                remap(channel, timestamp, remappedMirror, remapped);
            }
        } else if (!self.equals(remappedMirror)
                   && !remappedMirror.equals(originalPrimary)) {
            assert self.equals(originalMirror);
            // primary is up
            // Xerox state to the new mirror
            infoLog("Rebalancing for %s from old mirror %s to new mirror %s",
                    channel, self, remappedMirror);
            remap(channel, timestamp, remappedMirror, remapped);
        }
    }

    protected void remap(UUID channel, long timestamp, Node node,
                         HashMap<Node, List<UpdateState>> remapped) {
        UpdateState update = new UpdateState(channel, timestamp);
        List<UpdateState> updates = remapped.get(node);
        if (updates == null) {
            updates = new ArrayList<UpdateState>();
            remapped.put(node, updates);
        }
        updates.add(update);
    }

    private void infoLog(String logString, Object... args) {
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Rebalancing for %s from primary %s to new mirror %s",
                                   args));
        }
    }

    public void activate() {
        openPublishingGate();
    }

    public void inactivate() {
        // TODO - notify source of inactivation
        channelState.clear();
        mirrors.clear();
        spinners.clear();
        spinnerHandler.closeOpenHandlers();
    }
}
