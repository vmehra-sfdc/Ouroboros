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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.GlobalMessageType;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the producer node.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator implements Member {
    private final static Logger                           log          = Logger.getLogger(Coordinator.class.getCanonicalName());

    private AtomicReference<ConsistentHashFunction<Node>> weaverRing   = new AtomicReference<ConsistentHashFunction<Node>>(
                                                                                                                           new ConsistentHashFunction<Node>());
    private final SortedSet<Node>                         weavers      = new ConcurrentSkipListSet<Node>();
    private final Map<UUID, Spinner>                      channels     = new ConcurrentHashMap<UUID, Spinner>();
    private final SortedSet<UUID>                         mirrors      = new ConcurrentSkipListSet<UUID>();
    private final SortedSet<Node>                         newWeavers   = new ConcurrentSkipListSet<Node>();
    private final SortedSet<Node>                         newProducers = new ConcurrentSkipListSet<Node>();
    private final SortedSet<UUID>                         primary      = new ConcurrentSkipListSet<UUID>();
    private AtomicReference<ConsistentHashFunction<Node>> producerRing = new AtomicReference<ConsistentHashFunction<Node>>(
                                                                                                                           new ConsistentHashFunction<Node>());
    private final SortedSet<Node>                         producers    = new ConcurrentSkipListSet<Node>();
    private final Node                                    self;
    private final ConcurrentMap<Node, Spinner>            spinners     = new ConcurrentHashMap<Node, Spinner>();
    private final Switchboard                             switchboard;
    private final Map<Node, ContactInformation>           yellowPages  = new ConcurrentHashMap<Node, ContactInformation>();

    public Coordinator(Node self, Switchboard switchboard) {
        this.self = self;
        this.switchboard = switchboard;
    }

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(self,
                                         GlobalMessageType.ADVERTISE_PRODUCER,
                                         null));
    }

    /**
     * Close the channel
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        Spinner spinner = channels.remove(channel);
        if (spinner != null) {
            spinner.close(channel);
        }
    }

    @Override
    public void destabilize() {
    }

    @Override
    public void dispatch(GlobalMessageType type, Node sender,
                         Serializable payload, long time) {
        switch (type) {
            case ADVERTISE_CHANNEL_BUFFER:
                weavers.add(sender);
                yellowPages.put(sender, (ContactInformation) payload);
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
        // TODO Auto-generated method stub

    }

    /**
     * Perform the failover for this node. Failover to the channel mirrors and
     * assume primary producer responsibility for channels this node was
     * mirroring
     * 
     * @param deadMembers
     *            - the deceased
     * @return the List of channel ids that this node is now serving as the
     *         primary producer
     */
    public List<UUID> failover(Collection<Node> deadMembers) {
        // Initiate the failover proceedure for the dead spinners
        for (Iterator<Entry<Node, Spinner>> entries = spinners.entrySet().iterator(); entries.hasNext();) {
            Entry<Node, Spinner> entry = entries.next();
            if (deadMembers.contains(entry.getKey())) {
                entry.getValue().failover();
            }
        }

        // Failover mirror channels for which this node is now the primary
        ArrayList<UUID> newPrimaries = new ArrayList<UUID>();
        for (Iterator<UUID> mirrored = mirrors.iterator(); mirrored.hasNext();) {
            UUID channel = mirrored.next();
            Node[] producerPair = getProducerReplicationPair(channel);
            if (deadMembers.contains(producerPair[0])) {
                newPrimaries.add(channel);
                primary.add(channel);
                mirrored.remove();

                // Map the spinner for this channel
                Node[] channelPair = getChannelBufferReplicationPair(channel);
                Spinner spinner = spinners.get(channelPair[0]);
                if (spinner == null) { // primary is dead, so get mirror
                    spinner = spinners.get(channelPair[1]);
                }
                Spinner previous = channels.put(channel, spinner);
                assert previous == null : String.format("Apparently node %s is already primary for %");
            }
        }

        // Assign failover mirror for dead primaries
        for (Entry<UUID, Spinner> entry : channels.entrySet()) {
            Node[] pair = getChannelBufferReplicationPair(entry.getKey());
            if (deadMembers.contains(pair[0])) {
                Spinner newPrimary = spinners.get(entry.getKey());
                if (newPrimary == null) {
                    if (log.isLoggable(Level.WARNING)) {
                        log.warning(String.format("Both the primary and the secondary for %s have failed!",
                                                  entry.getKey()));
                    }
                } else {
                    Spinner failedPrimary = entry.getValue();
                    // Fail over to the new primary
                    for (Batch batch : failedPrimary.getPending(entry.getKey()).values()) {
                        newPrimary.push(batch);
                    }
                    entry.setValue(newPrimary);
                }
            }
        }

        // Remove dead spinners
        for (Iterator<Entry<Node, Spinner>> entries = spinners.entrySet().iterator(); entries.hasNext();) {
            Entry<Node, Spinner> entry = entries.next();
            if (deadMembers.contains(entry.getKey())) {
                entries.remove();
            }
        }
        return newPrimaries;
    }

    /**
     * Open the channel identified by the supplied id
     * 
     * @param channel
     *            - the unique id of the channel
     */
    public void open(UUID channel) {

    }

    @Override
    public void stabilized() {
        filterSystemMembership();
        failover(switchboard.getDeadMembers());
    }

    private void filterSystemMembership() {
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

    private Node[] getChannelBufferReplicationPair(UUID channel) {
        List<Node> pair = weaverRing.get().hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    private Node[] getProducerReplicationPair(UUID channel) {
        List<Node> pair = producerRing.get().hash(point(channel), 2);
        return new Node[] { pair.get(0), pair.get(1) };
    }

    /**
     * Update the consistent hash function for the weaver processs ring.
     * 
     * @param ring
     *            - the updated consistent hash function
     */
    public void updateRing(ConsistentHashFunction<Node> ring) {
        weaverRing.set(ring);
    }
}
