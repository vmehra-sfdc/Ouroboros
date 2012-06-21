/**
 * Copyright (c) 2012, salesforce.com, inc.
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
package com.salesforce.ouroboros.consumer;

import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * @author hhildebrand
 * 
 */
public class Consumer {
    private static final Logger                log                 = LoggerFactory.getLogger(Consumer.class);

    private ConsistentHashFunction<Node>       consumerRing;
    private final ConcurrentMap<UUID, Session> mirrorSubscriptions = new ConcurrentHashMap<>();
    private ConsistentHashFunction<Node>       nextConsumerRing;
    private final Node                         self;
    private final ConcurrentMap<UUID, Session> sessions            = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, Session> subscriptions       = new ConcurrentHashMap<>();
    private ConsistentHashFunction<Node>       weaverRing;

    public Consumer(Node self) {
        super();
        this.self = self;
    }

    public void cleanUp() {
        // TODO Auto-generated method stub

    }

    public void commitConsumerRing() {
        consumerRing = nextConsumerRing;
        nextConsumerRing = null;
    }

    public void createLooms(SortedSet<Node> activeWeavers,
                            Map<Node, ContactInformation> yellowPages) {
        // TODO Auto-generated method stub

    }

    public ConsistentHashFunction<Node> createRing() {
        // TODO Auto-generated method stub
        return null;
    }

    public void failover(Collection<Node> deadMembers)
                                                      throws InterruptedException {
        // TODO Auto-generated method stub

    }

    public Node getId() {
        return self;
    }

    public void inactivate() {
        // TODO Auto-generated method stub

    }

    public Map<UUID, Node[][]> remap() {
        // TODO Auto-generated method stub
        return null;
    }

    public void remapWeavers(ConsistentHashFunction<Node> ring) {
        // TODO Auto-generated method stub

    }

    public void setConsumerRing(ConsistentHashFunction<Node> ring) {
        // TODO Auto-generated method stub

    }

    /**
     * @param calculateNextProducerRing
     */
    public void setNextConsumerRing(ConsistentHashFunction<Node> calculateNextProducerRing) {
        // TODO Auto-generated method stub

    }
}
