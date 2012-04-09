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
package com.salesforce.ouroboros.producer.functional;

import static com.salesforce.ouroboros.testUtils.Util.waitFor;
import static com.salesforce.ouroboros.util.Utils.point;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.views.BitView;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.hellblazer.jackal.testUtil.TestController;
import com.hellblazer.jackal.testUtil.TestNode;
import com.hellblazer.jackal.testUtil.TestNodeCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipControllerCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipTestCfg;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerCoordinator;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.testUtils.Util.Condition;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.MersenneTwister;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestProducerCluster {

    static final Logger                              log     = LoggerFactory.getLogger(TestProducerCluster.class.getCanonicalName());

    private ClusterMaster                            clusterMaster;
    private AnnotationConfigApplicationContext       clusterMasterContext;
    private final Class<?>[]                         configs = new Class[] {
            weaver0.class, weaver.class, weaver2.class      };
    private TestController                           controller;
    private AnnotationConfigApplicationContext       controllerContext;
    private List<ProducerCoordinator>                coordinators;
    private List<AnnotationConfigApplicationContext> fakeSpindleContexts;
    private Node[]                                   fakeSpindleNodes;
    private List<TestNode>                           fullPartition;
    private List<Node>                               fullPartitionId;
    private BitView                                  fullView;
    private List<TestNode>                           majorGroup;
    private List<ProducerCoordinator>                majorPartition;
    private List<Node>                               majorPartitionId;
    private List<Producer>                           majorProducers;
    private BitView                                  majorView;
    private List<AnnotationConfigApplicationContext> memberContexts;
    private List<TestNode>                           minorGroup;
    private List<ProducerCoordinator>                minorPartition;
    private List<Node>                               minorPartitionId;
    private List<Producer>                           minorProducers;
    private BitView                                  minorView;
    private List<Producer>                           producers;
    private MersenneTwister                          twister = new MersenneTwister(
                                                                                   666);

    static {
        GossipTestCfg.setTestPorts(25230, 23546);
    }

    @Before
    public void startUp() throws Exception {
        TestNodeCfg.nextMagic();
        FakeSpindle.PORT++;
        GossipTestCfg.incrementPorts();
        weaver.reset();
        log.info("Setting up initial partition");
        CountDownLatch initialLatch = new CountDownLatch(configs.length + 3);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   GossipControllerCfg.class);
        controller = controllerContext.getBean(TestController.class);
        controller.cardinality = configs.length + 3;
        controller.latch = initialLatch;
        clusterMasterContext = new AnnotationConfigApplicationContext(
                                                                      ClusterMasterCfg.class);
        clusterMaster = clusterMasterContext.getBean(ClusterMaster.class);
        fakeSpindleContexts = new ArrayList<AnnotationConfigApplicationContext>();
        fakeSpindleContexts.add(new AnnotationConfigApplicationContext(
                                                                       FakeSpindleCfg.class));
        fakeSpindleContexts.add(new AnnotationConfigApplicationContext(
                                                                       FakeSpindleCfg.class));
        memberContexts = createMembers();
        log.info("Awaiting initial partition stability");
        boolean success = false;
        try {
            success = initialLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Initial partition did not acheive stability", success);
            log.info("Initial partition stable");
            fullPartition = new ArrayList<TestNode>();
            coordinators = new ArrayList<ProducerCoordinator>();
            producers = new ArrayList<Producer>();
            fullPartitionId = new ArrayList<Node>();
            for (AnnotationConfigApplicationContext context : memberContexts) {
                TestNode node = (TestNode) controller.getNode(context.getBean(Identity.class));
                assertNotNull("Can't find node: "
                                      + context.getBean(Identity.class), node);
                fullPartition.add(node);
                ProducerCoordinator coordinator = context.getBean(ProducerCoordinator.class);
                fullPartitionId.add(coordinator.getId());
                assertNotNull("Can't find coordinator in context: " + context,
                              coordinator);
                coordinators.add(coordinator);
                producers.add(context.getBean(Producer.class));
            }
        } finally {
            if (!success) {
                tearDown();
            }
        }

        TestNode clusterMasterNode = (TestNode) controller.getNode(clusterMasterContext.getBean(Identity.class));
        fullPartition.add(clusterMasterNode);

        assertPartitionBootstrapping(coordinators);

        majorView = new BitView();
        minorView = new BitView();
        fullView = new BitView();

        ArrayList<Node> nodes = new ArrayList<Node>();
        for (AnnotationConfigApplicationContext ctxt : fakeSpindleContexts) {
            Identity id = ctxt.getBean(Identity.class);
            fullPartition.add((TestNode) controller.getNode(id));
            fullView.add(id);
            majorView.add(id);
            nodes.add(ctxt.getBean(Node.class));
        }
        fakeSpindleNodes = nodes.toArray(new Node[nodes.size()]);

        majorPartition = new ArrayList<ProducerCoordinator>();
        minorPartition = new ArrayList<ProducerCoordinator>();

        majorGroup = new ArrayList<TestNode>();
        minorGroup = new ArrayList<TestNode>();

        majorPartitionId = new ArrayList<Node>();
        minorPartitionId = new ArrayList<Node>();

        majorProducers = new ArrayList<Producer>();
        minorProducers = new ArrayList<Producer>();

        int majorPartitionSize = coordinators.size() / 2 + 1;

        // Form the major partition
        for (int i = 0; i < majorPartitionSize; i++) {
            TestNode member = fullPartition.get(i);
            ProducerCoordinator coordinator = coordinators.get(i);
            majorPartitionId.add(fullPartitionId.get(i));
            majorPartition.add(coordinator);
            fullView.add(member.getIdentity());
            majorGroup.add(member);
            majorView.add(member.getIdentity());
            majorProducers.add(producers.get(i));
        }

        majorGroup.add(clusterMasterNode);
        fullView.add(clusterMasterNode.getIdentity());
        majorView.add(clusterMasterNode.getIdentity());
        for (AnnotationConfigApplicationContext ctxt : fakeSpindleContexts) {
            Identity id = ctxt.getBean(Identity.class);
            majorGroup.add((TestNode) controller.getNode(id));
        }

        // Form the minor partition
        for (int i = majorPartitionSize; i < coordinators.size(); i++) {
            TestNode member = fullPartition.get(i);
            ProducerCoordinator coordinator = coordinators.get(i);
            minorPartitionId.add(coordinator.getId());
            minorPartition.add(coordinator);
            fullView.add(member.getIdentity());
            minorGroup.add(member);
            minorView.add(member.getIdentity());
            minorProducers.add(producers.get(i));
        }

        log.info(String.format("Major partition %s, minor partition %s",
                               majorView, minorView));
    }

    @After
    public void tearDown() throws Exception {
        if (controllerContext != null) {
            try {
                controllerContext.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        controllerContext = null;
        if (memberContexts != null) {
            for (AnnotationConfigApplicationContext context : memberContexts) {
                try {
                    context.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
        memberContexts = null;
        controller = null;
        fullPartition = null;
    }

    /**
     * Test the partitioning behavior of the Coordinator. Test that we can
     * asymmetrically partition the coordinators and that the stable partition
     * stablizes. Then test that we can reform the partition and the reformed
     * partition stabilzed. Test that the only active members are those that
     * were part of the minority partition that could stabilize.
     * 
     * @throws Exception
     */
    @Test
    public void testPartitioning() throws Exception {
        bootstrap();

        asymmetricallyPartition();

        // major partition should be active and awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        assertPartitionActive(majorPartition);

        log.info("Failing over");
        clusterMaster.prepare();
        clusterMaster.failover();

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

        reformPartition();

        // Check to see everything is kosher
        for (TestNode member : fullPartition) {
            assertEquals(fullView, member.getPartition());
        }

        // Major partition should be awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        // Minor partition should be stable
        assertPartitionStable(minorPartition);

        clusterMaster.prepare();
        clusterMaster.failover();

        // Only the major partition should be active 
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);

        // Entire partition should be stable
        assertPartitionStable(coordinators);
    }

    /**
     * Test the failover and rebalancing behavior of the Coordinator. Open a
     * number of channels on the producers responsible for managing them, push
     * some event batches to each. Partition the system and assert that the
     * remaining producers failed over correctly. Rebalance the remaining
     * producers and verify that the rebalance was performed correctly.
     * Recombine the partition, activate the minor partition members, and assert
     * the correct rebalancing of the producers.
     * 
     * @throws Exception
     */
    @Test
    public void testRebalance() throws Exception {
        bootstrap();

        assertTrue("Did not receive the BOOTSTRAP_SPINDLES message",
                   clusterMaster.bootstrapSpindles(fakeSpindleNodes, 60,
                                                   TimeUnit.SECONDS));

        ArrayList<UUID> channels = getChannelIds();

        ConsistentHashFunction<Node> ring = producers.get(0).createRing();
        for (Producer p : producers) {
            Node n = p.getId();
            ring.add(n, n.capacity);
        }

        // Open the channels
        for (UUID channel : channels) {
            log.info(String.format("Opening channel %s", channel));
            assertTrue("Channel OPEN message not received",
                       clusterMaster.open(channel, 60, TimeUnit.SECONDS));
        }

        assertChannelMappings(channels, producers, ring);

        asymmetricallyPartition();

        // major partition should be active and awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        assertPartitionActive(majorPartition);

        log.info("Failing over");
        clusterMaster.prepare();
        clusterMaster.failover();

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

        // Filter out all the channels with a primary and secondary on the minor partition
        List<UUID> lostChannels = filterChannels(channels, ring);

        assertFailoverChannelMappings(channels, majorPartitionId,
                                      majorProducers, ring);

        validateLostChannels(lostChannels, majorProducers);

        // Rebalance the open channels across the major partition
        log.info("Initiating rebalance on majority partition");
        ProducerCoordinator partitionLeader = majorPartition.get(majorPartition.size() - 1);
        assertTrue("coordinator is not the leader",
                   partitionLeader.isActiveLeader());
        partitionLeader.initiateRebalance();

        assertPartitionStable(majorPartition);

        reformPartition();

        // Check to see everything is kosher
        for (TestNode member : fullPartition) {
            assertEquals(fullView, member.getPartition());
        }

        // Major partition should be awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        // Minor partition should be stable
        assertPartitionStable(minorPartition);

        clusterMaster.prepare();
        clusterMaster.failover();

        // Only the major partition should be active 
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);

        // Entire partition should be stable
        assertPartitionStable(coordinators);

        ring = producers.get(0).createRing();
        for (Node node : majorPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, majorProducers, ring);

        validateLostChannels(lostChannels, majorProducers);

        log.info("Activating minor partition");
        // Now add the minor partition back into the active set
        ArrayList<Node> minorPartitionNodes = new ArrayList<Node>();
        for (ProducerCoordinator c : minorPartition) {
            minorPartitionNodes.add(c.getId());
        }
        partitionLeader.initiateRebalance(minorPartitionNodes.toArray(new Node[0]));

        // Entire partition should be stable 
        assertPartitionStable(coordinators);

        // Entire partition should be active
        assertPartitionActive(coordinators);

        // Everything should be back to normal
        ring = producers.get(0).createRing();
        for (Node node : fullPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, producers, ring);

        // validate lost channels aren't mapped on the partition members
        validateLostChannels(lostChannels, producers);
    }

    private void asymmetricallyPartition() throws InterruptedException {
        CountDownLatch latchA = latch(majorGroup);
        log.info("Asymmetrically partitioning");
        controller.asymPartition(majorView);

        log.info("Awaiting stability of minor partition A");
        assertTrue("major partition did not stabilize",
                   latchA.await(60, TimeUnit.SECONDS));

        log.info("Major partition has stabilized");

        // Check to see everything is as expected.
        for (TestNode member : majorGroup) {
            assertEquals(majorView, member.getPartition());
        }
    }

    private void bootstrap() throws InterruptedException {
        log.info("Bootstrapping the producers");
        coordinators.get(coordinators.size() - 1).initiateBootstrap();

        assertPartitionStable(coordinators);
        assertPartitionActive(coordinators);
    }

    private List<AnnotationConfigApplicationContext> createMembers() {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
    }

    private List<UUID> filterChannels(ArrayList<UUID> channels,
                                      ConsistentHashFunction<Node> ring) {
        List<UUID> lostChannels = new ArrayList<UUID>();
        for (UUID channel : channels) {
            if (minorPartitionId.containsAll(ring.hash(point(channel), 2))) {
                lostChannels.add(channel);
            }
        }
        channels.removeAll(lostChannels);
        return lostChannels;
    }

    private ArrayList<UUID> getChannelIds() {
        int numOfChannels = 100;
        ArrayList<UUID> channels = new ArrayList<UUID>();
        for (int i = 0; i < numOfChannels; i++) {
            UUID channel = new UUID(twister.nextLong(), twister.nextLong());
            channels.add(channel);
        }
        return channels;
    }

    private void reformPartition() throws InterruptedException {

        // reform
        CountDownLatch latch = new CountDownLatch(fullPartition.size());
        for (TestNode node : fullPartition) {
            node.latch = latch;
            node.cardinality = fullPartition.size();
        }
        // Clear the partition
        log.info("Reforming partition");
        controller.clearPartitions();

        log.info("Awaiting stability of reformed partition");
        assertTrue("Full partition did not stablize",
                   latch.await(60, TimeUnit.SECONDS));

        log.info("Full partition has stabilized");
    }

    private void validateLostChannels(List<UUID> lostChannels,
                                      List<Producer> producers) {
        for (UUID channel : lostChannels) {
            for (Producer producer : producers) {
                assertFalse(String.format("%s should not be hosting lost channel %s",
                                          producer.getId(), channel),
                            producer.isActingPrimaryFor(channel)
                                    || producer.isActingPrimaryFor(channel));
            }
        }
    }

    protected void assertChannelMappings(List<UUID> channels,
                                         List<Producer> producers,
                                         ConsistentHashFunction<Node> ring) {
        for (UUID channel : channels) {
            List<Node> mapping = ring.hash(point(channel), 2);
            Node primary = mapping.get(0);
            Node mirror = mapping.get(1);
            for (Producer producer : producers) {
                if (primary.equals(producer.getId())) {
                    assertTrue(String.format("%s should be the primary for %s, but doesn't host it",
                                             primary, channel),
                               producer.isActingPrimaryFor(channel));
                    assertFalse(String.format("%s should be the primary for %s, but believes it is also the mirror",
                                              primary, channel),
                                producer.isActingMirrorFor(channel));
                } else {
                    if (mirror.equals(producer.getId())) {
                        boolean failed = true;
                        for (Producer p : producers) {
                            if (p.getId().equals(primary)) {
                                failed = false;
                                break;
                            }
                        }
                        if (failed) {
                            assertTrue(String.format("%s should be the fail over primary for %s",
                                                     mirror, channel),
                                       producer.isActingPrimaryFor(channel));
                            assertFalse(String.format("%s should not be the mirror for %s",
                                                      mirror, channel),
                                        producer.isActingMirrorFor(channel));
                        } else {
                            assertTrue(String.format("%s should be the mirror for %s, but doesn't host it",
                                                     mirror, channel),
                                       producer.isActingMirrorFor(channel));
                            assertFalse(String.format("%s should be the mirror for %s, but is primary",
                                                      mirror, channel),
                                        producer.isActingPrimaryFor(channel));
                        }
                    } else {
                        assertFalse(String.format("%s claims to host %s as primary, but isn't marked as primary or mirror",
                                                  producer.getId(), channel),
                                    producer.isActingPrimaryFor(channel));
                        assertFalse(String.format("%s claims to host %s as mirror, but isn't marked as mirror",
                                                  producer.getId(), channel),
                                    producer.isActingMirrorFor(channel));
                    }
                }
            }
        }
    }

    protected void assertFailoverChannelMappings(List<UUID> channels,
                                                 List<Node> idGroup,
                                                 List<Producer> producerGroup,
                                                 ConsistentHashFunction<Node> ring) {
        for (UUID channel : channels) {
            List<Node> mapping = ring.hash(point(channel), 2);
            Node primary = mapping.get(0);
            Node mirror = mapping.get(1);
            for (Producer producer : producerGroup) {
                if (!idGroup.contains(primary)) {
                    if (mirror.equals(producer.getId())) {
                        assertTrue(String.format("%s should be the failed over primary for %s, but doesn't host it",
                                                 mirror, channel),
                                   producer.isActingPrimaryFor(channel));
                        assertFalse(String.format("%s should be the failed over primary for %s, but is mirror",
                                                  mirror, channel),
                                    producer.isActingMirrorFor(channel));
                    }
                } else if (!idGroup.contains(mirror)) {
                    if (primary.equals(producer.getId())) {
                        assertTrue(String.format("%s should remain the primary for %s, but doesn't host it",
                                                 primary, channel),
                                   producer.isActingPrimaryFor(channel));
                        assertFalse(String.format("%s should remain the primary for %s, but is mirror",
                                                  primary, channel),
                                    producer.isActingMirrorFor(channel));
                    }
                } else {
                    if (primary.equals(producer.getId())) {
                        assertTrue(String.format("%s should be the primary for %s, but doesn't host it",
                                                 primary, channel),
                                   producer.isActingPrimaryFor(channel));
                        assertFalse(String.format("%s should be the primary for %s, but is mirror",
                                                  primary, channel),
                                    producer.isActingMirrorFor(channel));
                    } else {
                        if (mirror.equals(producer.getId())) {
                            assertTrue(String.format("%s should be the mirror for %s, but doesn't host it",
                                                     mirror, channel),
                                       producer.isActingMirrorFor(channel));
                            assertFalse(String.format("%s should be the mirror for %s, but is primary",
                                                      mirror, channel),
                                        producer.isActingPrimaryFor(channel));
                        } else {
                            assertFalse(String.format("%s claims to host %s, but isn't marked as primary or mirror",
                                                      producer.getId(), channel),
                                        producer.isActingMirrorFor(channel)
                                                || producer.isActingPrimaryFor(channel));
                        }
                    }
                }
            }
        }
    }

    protected void assertPartitionActive(List<ProducerCoordinator> partition)
                                                                             throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the active state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.isActive();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionAwaitingFailover(List<ProducerCoordinator> partition)
                                                                                       throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the awaiting failover state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.getState() == CoordinatorFSM.AwaitingFailover;
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionBootstrapping(List<ProducerCoordinator> partition)
                                                                                    throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return CoordinatorFSM.Bootstrapping == c.getState()
                           || ControllerFSM.Bootstrap == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionInactive(List<ProducerCoordinator> partition)
                                                                               throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the inactive state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return !c.isActive();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionStable(List<ProducerCoordinator> partition)
                                                                             throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the stable state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return CoordinatorFSM.Stable == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected CountDownLatch latch(List<TestNode> group) {
        CountDownLatch latch = new CountDownLatch(group.size());
        for (TestNode member : group) {
            member.latch = latch;
            member.cardinality = group.size();
        }
        return latch;
    }
}
