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
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.ChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.ChannelMessage;
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.api.producer.UnknownChannelException;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.Gate;
import com.salesforce.ouroboros.util.rate.Controller;
import com.salesforce.ouroboros.util.rate.controllers.RateController;
import com.salesforce.ouroboros.util.rate.controllers.RateLimiter;

/**
 * The distributed coordinator for the producer node.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {

    public class UpdateState {
        public final UUID channel;
        public final long timestamp;

        public UpdateState(UUID channel, long timestamp) {
            super();
            this.channel = channel;
            this.timestamp = timestamp;
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

    private final static Logger                                 log               = Logger.getLogger(Coordinator.class.getCanonicalName());

    private final ConcurrentMap<UUID, ChannelState>             channelState      = new ConcurrentHashMap<UUID, ChannelState>();
    private final Controller                                    controller;
    private final CoordinatorContext                            fsm               = new CoordinatorContext(
                                                                                                           this);
    private final Map<UUID, Long>                               mirrors           = new ConcurrentHashMap<UUID, Long>();
    private final SortedSet<Node>                               newProducers      = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>                               newWeavers        = new ConcurrentSkipListSet<Node>();
    private final AtomicReference<ConsistentHashFunction<Node>> producerRing      = new AtomicReference<ConsistentHashFunction<Node>>(
                                                                                                                                      new ConsistentHashFunction<Node>());
    private final SortedSet<Node>                               producers         = new ConcurrentSkipListSet<Node>();
    private final Gate                                          publishGate       = new Gate();
    private final AtomicInteger                                 publishingThreads = new AtomicInteger(
                                                                                                      0);
    private final Node                                          self;
    private final EventSource                                   source;
    private final ChannelHandler                                spinnerHandler;
    private final ConcurrentMap<Node, Spinner>                  spinners          = new ConcurrentHashMap<Node, Spinner>();
    private final Switchboard                                   switchboard;
    private final AtomicReference<ConsistentHashFunction<Node>> weaverRing        = new AtomicReference<ConsistentHashFunction<Node>>(
                                                                                                                                      new ConsistentHashFunction<Node>());
    private final SortedSet<Node>                               weavers           = new ConcurrentSkipListSet<Node>();
    private final Map<Node, ContactInformation>                 yellowPages       = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(Node self, Switchboard switchboard, EventSource source,
                       CoordinatorConfiguration configuration)
                                                              throws IOException {
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
        this.switchboard = switchboard;
        switchboard.setMember(this);
        this.source = source;
        spinnerHandler = new ChannelHandler(
                                            "Spinner Handler",
                                            configuration.getSpinnerSocketOptions(),
                                            configuration.getSpinners());
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

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(self,
                                         GlobalMessageType.ADVERTISE_PRODUCER));
    }

    /**
     * Close the channel
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        Node[] pair = getChannelBufferReplicationPair(channel);
        Node primary;
        if (weavers.contains(pair[0])) {
            primary = pair[0];
        } else {
            primary = pair[1];
        }
        switchboard.send(new Message(self, ChannelMessage.CLOSE), primary);
    }

    public void closed(UUID channel) {
        ChannelState state = channelState.remove(channel);
        if (state != null) {
            state.spinner.close(channel);
        }
        source.closed(channel);
    }

    @Override
    public void destabilize() {
        fsm.destabilized();
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable payload, long time) {
        switch (type) {
            case OPENED: {
                opened((UUID) payload);
                break;
            }
            case CLOSED: {
                break;
            }
            default: {
                if (log.isLoggable(Level.WARNING)) {
                    log.warning(String.format("Invalid target %s for channel message: %s",
                                              self, type));
                }
            }
        }
    }

    @Override
    public void dispatch(GlobalMessageType type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                weavers.add(sender);
                yellowPages.put(sender, (ContactInformation) arguments[0]);
                break;
            case ADVERTISE_PRODUCER:
                producers.add(sender);
                break;
            default:
                break;
        }
    }

    @Override
    public void dispatch(MemberDispatch type, Node sender,
                         Serializable payload, long time) {
    }

    /**
     * Answer the node represting this coordinator's id
     * 
     * @return the Node representing this process
     */
    public Node getId() {
        return self;
    }

    /**
     * Open the channel identified by the supplied id. The channel is not open
     * until the opened event is sent to the caller
     * 
     * @param channel
     *            - the unique id of the channel
     * @throws IllegalArgumentException
     *             if this coordinator is not the primary producer responsible
     *             for this process
     */
    public void open(UUID channel) {
        Node[] producerPair = getProducerReplicationPair(channel);
        if (self.equals(producerPair[0])) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("%s is primary for channel %s, opening",
                                       self, channel));
            }
        } else if (!producers.contains(producerPair[0])
                   && self.equals(producerPair[1])) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("%s is mirror for channel %s, primary %s does not exist, opening",
                                       self, channel, producerPair[0]));
            }
        } else {
            throw new IllegalArgumentException(
                                               String.format("%s is not the primary for channel %s",
                                                             self, channel));
        }
        Node[] pair = getChannelBufferReplicationPair(channel);
        Node primary;
        if (weavers.contains(pair[0])) {
            primary = pair[0];
        } else {
            primary = pair[1];
        }
        switchboard.send(new Message(self, ChannelMessage.OPEN), primary);
    }

    /**
     * The channel has been opened.
     * 
     * @param channel
     *            - the id of the channel
     */
    public void opened(UUID channel) {
        mapSpinner(channel);
        source.opened(channel);
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
                    log.info(String.format("Push to a channel which does not exist: %s",
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
            Node[] pair = getProducerReplicationPair(channel);
            if (!state.spinner.push(new Batch(pair[1], channel, timestamp,
                                              events))) {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Rate limited, as service temporarily down for channel: %s",
                                           channel));
                }
                throw new RateLimiteExceededException(
                                                      String.format("The system cannot currently service the push request"));
            }
        } finally {
            publishingThreads.decrementAndGet();
        }
    }

    /**
     * Remap the producers by incorporating the set of new producers into the
     * consistent hash ring of producers. This remapping may result in event
     * channels that this node is serving as the primary have now been moved to
     * a new primary.
     * 
     * @return the update mappings for channels that are moved to new primaries.
     * @throws InterruptedException
     */
    public Map<Node, List<UpdateState>> remapProducers() {
        ConsistentHashFunction<Node> newRing = new ConsistentHashFunction<Node>();
        for (Node producer : producers) {
            newRing.add(producer, producer.capacity);
        }
        for (Node producer : producers) {
            newRing.add(producer, producer.capacity);
        }
        for (Node producer : newProducers) {
            producers.add(producer);
            newRing.add(producer, producer.capacity);
        }
        newProducers.clear();
        HashMap<Node, List<UpdateState>> remapped = new HashMap<Node, List<UpdateState>>();
        for (Iterator<Entry<UUID, ChannelState>> mappings = channelState.entrySet().iterator(); mappings.hasNext();) {
            Entry<UUID, ChannelState> mapping = mappings.next();
            Node primary = newRing.hash(point(mapping.getKey()));
            if (!primary.equals(self)) {
                List<UpdateState> updates = remapped.get(primary);
                if (updates == null) {
                    updates = new ArrayList<UpdateState>();
                    remapped.put(primary, updates);
                }
                updates.add(new UpdateState(mapping.getKey(),
                                            mapping.getValue().timestamp));
                mappings.remove();
            }
        }
        return remapped;
    }

    /**
     * Remap the spinners by incorporating the set of new weavers into the
     * consistent hash ring. This amounts to recalculating the weaver consistent
     * hash ring and then doing the equivalent of a failover to the newly mapped
     * primaries: all currently pending pushes for the old primaries will be
     * reenqueued on the new primaries.
     */
    public void remapWeavers() {
        ConsistentHashFunction<Node> newRing = new ConsistentHashFunction<Node>();
        for (Node weaver : weavers) {
            newRing.add(weaver, weaver.capacity);
        }
        for (Node weaver : newWeavers) {
            weavers.add(weaver);
            newRing.add(weaver, weaver.capacity);
        }
        newWeavers.clear();
        for (Entry<UUID, ChannelState> mapping : channelState.entrySet()) {
            Spinner remapped = spinners.get(newRing.hash(point(mapping.getKey())));
            if (remapped != mapping.getValue().spinner) {
                mapping.getValue().spinner = remapped;
                for (Batch batch : mapping.getValue().spinner.getPending(mapping.getKey()).values()) {
                    remapped.push(batch);
                }
            }
        }
        weaverRing.set(newRing);
    }

    @Override
    public void stabilized() {
        fsm.stabilized();
    }

    /**
     * Start the coordinator
     */
    public void start() {
        spinnerHandler.start();
    }

    /**
     * Terminate the coordinator
     */
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
     * @return the Map of channel ids and their last committed timestamp that
     *         this node is now serving as the primary producer
     * @throws InterruptedException
     *             - if the thread is interrupted
     */
    private Map<UUID, Long> failover(Collection<Node> deadMembers)
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
                newPrimaries.put(channel, entry.getValue());
                mirrored.remove();

                // Map the spinner for this channel
                Node[] channelPair = getChannelBufferReplicationPair(channel);
                Spinner spinner = spinners.get(channelPair[0]);
                if (spinner == null) { // primary is dead, so get mirror
                    spinner = spinners.get(channelPair[1]);
                }
                ChannelState previous = channelState.put(channel,
                                                         new ChannelState(
                                                                          spinner,
                                                                          entry.getValue()));
                assert previous == null : String.format("Apparently node %s is already primary for %");
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
                        newPrimary.push(batch);
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

        // Let any publishing threads preceed
        openPublishingGate();

        return newPrimaries;
    }

    /**
     * Answer the channel buffer process pair responsible for the channel
     * 
     * @param channel
     *            - the id of the channel
     * @return the array of length 2, containing the primary and mirror Node
     *         responsible for the channel
     */
    private Node[] getChannelBufferReplicationPair(UUID channel) {
        List<Node> pair = weaverRing.get().hash(point(channel), 2);
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
    private Node[] getProducerReplicationPair(UUID channel) {
        List<Node> pair = producerRing.get().hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    private void mapSpinner(UUID channel) {
        Node[] channelPair = getChannelBufferReplicationPair(channel);
        Spinner spinner = spinners.get(channelPair[0]);
        if (spinner == null) {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Primary node %s for channel %s is down, mapping mirror %s",
                                       channelPair[0], channel, channelPair[1]));
            }
            spinner = spinners.get(channelPair[1]);
            if (spinner == null) {
                if (log.isLoggable(Level.SEVERE)) {
                    log.severe(String.format("Mirror %s for channel %s is down",
                                             channelPair[0], channel));
                }
            } else {
                if (log.isLoggable(Level.INFO)) {
                    log.info(String.format("Mapping channel %s to mirror $s",
                                           channel, channelPair[1]));
                }
                channelState.putIfAbsent(channel, new ChannelState(spinner, -1));
            }
        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info(String.format("Mapping channel %s to primary $s",
                                       channel, channelPair[0]));
            }
            channelState.putIfAbsent(channel, new ChannelState(spinner, -1));
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
    protected void createSpinners() {
        for (Node n : newWeavers) {
            ContactInformation info = yellowPages.get(n);
            assert info != null : String.format("Did not find any connection information for node %s",
                                                n);
            Spinner spinner = new Spinner(this);
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

    protected void failover() {
        Map<UUID, Long> newPrimaries;
        try {
            newPrimaries = failover(switchboard.getDeadMembers());
        } catch (InterruptedException e) {
            return;
        }
        if (newPrimaries.size() != 0) {
            source.assumePrimary(newPrimaries);
        }
    }

    /**
     * Remove all dead members and partition out the new members from the
     * members that were part of the previous partition
     */
    protected void filterSystemMembership() {
        producers.removeAll(switchboard.getDeadMembers());
        weavers.removeAll(switchboard.getDeadMembers());
        newProducers.clear();
        newWeavers.clear();
        for (Node node : switchboard.getNewMembers()) {
            if (producers.remove(node)) {
                newProducers.add(node);
            }
            if (weavers.remove(node)) {
                newProducers.add(node);
            }
        }
    }

    protected void openPublishingGate() {
        publishGate.open();
    }
}
