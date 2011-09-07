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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.locator.AnubisListener;
import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.locator.AnubisProvider;
import org.smartfrog.services.anubis.locator.AnubisStability;
import org.smartfrog.services.anubis.locator.AnubisValue;

import com.salesforce.ouroboros.ReplicationPair;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * The distributed coordinator for the system.
 * 
 * @author hhildebrand
 * 
 */
public class Coordinator {
    public enum Role {
        MIRROR, PRIMARY
    }

    public enum State {
        INITIAL, JOINED, JOINING
    }

    /**
     * 
     * Provides the distributed orchestration for the weaver processes
     * 
     */
    class Orchestrator extends AnubisListener {
        private class Stability extends AnubisStability {
            @Override
            public void stability(boolean isStable, long timeRef) {
                // TODO Auto-generated method stub
            }
        }

        private final AnubisLocator  locator;
        private final Stability      stability;
        private final AnubisProvider stateProvider;

        public Orchestrator(String stateName, AnubisLocator anubisLocator) {
            super(stateName);
            locator = anubisLocator;
            stateProvider = new AnubisProvider(stateName);
            stability = new Stability();
            locator.registerStability(stability);
            locator.registerProvider(stateProvider);
            locator.registerListener(this);
        }

        @Override
        public void newValue(AnubisValue value) {
            if (value.getValue() == null) {
                return; // Initial value
            }
        }

        @Override
        public void removeValue(AnubisValue value) {
            // TODO Auto-generated method stub
        }
    }

    private final static Logger                log            = Logger.getLogger(Coordinator.class.getCanonicalName());

    private final Map<UUID, ReplicationPair>   channelMapping = new HashMap<UUID, ReplicationPair>();
    private Weaver                             localWeaver;
    private final Set<UUID>                    subscriptions  = new HashSet<UUID>();
    private final ConsistentHashFunction<Node> weaverRing     = new ConsistentHashFunction<Node>();
    private final Map<Node, ContactInfomation> yellowPages    = new HashMap<Node, ContactInfomation>();
    final Orchestrator                         orchestrator;

    public Coordinator(String stateName, AnubisLocator locator) {
        orchestrator = new Orchestrator(stateName, locator);
    }

    public void closeSubscription(UUID id) {
        ReplicationPair pair = channelMapping.get(id);
        if (pair != null) {
            int processId = localWeaver.getId().processId;
            if (pair.getMirror() == processId || pair.getPrimary() == processId) {
                localWeaver.close(id);
            }
        }
    }

    public ContactInfomation getContactInformationFor(Node node) {
        return yellowPages.get(node);
    }

    public Node[] getReplicationPair(UUID channelId) {
        // TODO Auto-generated method stub
        return null;
    }

    public void join(Node id) {
        weaverRing.add(id, id.capacity);
    }

    public void openSubscription(UUID channel) {
        subscriptions.add(channel);
        List<Node> pair = weaverRing.hash(point(channel), 2);
        channelMapping.put(channel, new ReplicationPair(pair.get(0).processId,
                                                        pair.get(1).processId));
        if (pair.get(0).equals(localWeaver.getId())) {
            localWeaver.openPrimary(channel, pair.get(1));
        } else if (pair.get(0).equals(localWeaver.getId())) {
            localWeaver.openMirror(channel, pair.get(0));
        }
    }

    /**
     * The weaver membership has partitioned. Clear out the dead members,
     * accomidate the new members and normalize the state
     * 
     * @param deadMembers
     *            - the weaver nodes that have failed and are no longer part of
     *            the partition
     * @param newMembers
     *            - the new weaver nodes that are now part of the partition
     */
    public void partition(Collection<Node> deadMembers,
                          Map<Node, ContactInfomation> newMembers) {
        for (Node node : deadMembers) {
            if (log.isLoggable(Level.INFO)) {
                log.fine(String.format("Removing weaver[%s] from the partition",
                                       node));
            }
            yellowPages.remove(node);
            localWeaver.closeReplicator(node);
        }
        yellowPages.putAll(newMembers);
        for (Entry<Node, ContactInfomation> entry : newMembers.entrySet()) {
            localWeaver.openReplicator(entry.getKey(), entry.getValue());
        }
    }

    public void ready(Weaver weaver) {
        localWeaver = weaver;
    }

    private long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
    }
}
