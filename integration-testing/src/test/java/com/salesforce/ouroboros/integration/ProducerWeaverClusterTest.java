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
package com.salesforce.ouroboros.integration;

import static com.salesforce.ouroboros.testUtils.Util.waitFor;
import static com.salesforce.ouroboros.util.Utils.point;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.views.BitView;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.testUtil.TestController;
import com.hellblazer.jackal.testUtil.TestNode;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.integration.util.ClusterControllerCfg;
import com.salesforce.ouroboros.integration.util.ClusterDiscoveryNode1Cfg;
import com.salesforce.ouroboros.integration.util.ClusterDiscoveryNode2Cfg;
import com.salesforce.ouroboros.integration.util.ClusterDiscoveryNode3Cfg;
import com.salesforce.ouroboros.integration.util.ClusterDiscoveryNode4Cfg;
import com.salesforce.ouroboros.integration.util.ClusterNodeCfg;
import com.salesforce.ouroboros.integration.util.ClusterTestCfg;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerCoordinator;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.producer.ProducerCoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.WeaverCoordinator;
import com.salesforce.ouroboros.spindle.WeaverCoordinatorContext;
import com.salesforce.ouroboros.testUtils.Util.Condition;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.MersenneTwister;

/**
 * @author hhildebrand
 * 
 */
public class ProducerWeaverClusterTest {
    private static final int BATCH_SIZE    = 10;
    private static final int BATCH_COUNT   = 500;
    private static final int CHANNEL_COUNT = 100;

    @Configuration
    static class master extends ClusterNodeCfg {

        private int node = -1;

        @Bean
        @Autowired
        public ClusterMaster clusterMaster(Switchboard switchboard) {
            return new ClusterMaster(switchboard);
        }

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }

        @Bean
        @Autowired
        public Switchboard switchboard(Partition partitionManager) {
            int identity = partitionIdentity().id;
            Node memberNode = new Node(identity, identity, identity);
            Switchboard switchboard = new Switchboard(
                                                      memberNode,
                                                      partitionManager,
                                                      Generators.timeBasedGenerator());
            return switchboard;
        }
    }

    @Configuration
    @Import(ProducerCfg.class)
    static class producer extends ClusterNodeCfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(ProducerCfg.class)
    static class producer1 extends ClusterDiscoveryNode1Cfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(ProducerCfg.class)
    static class producer2 extends ClusterDiscoveryNode3Cfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(WeaverCfg.class)
    static class weaver extends ClusterNodeCfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(WeaverCfg.class)
    static class weaver1 extends ClusterDiscoveryNode2Cfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(WeaverCfg.class)
    static class weaver2 extends ClusterDiscoveryNode4Cfg {

        private int node = -1;

        @Override
        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    private static AtomicInteger id  = new AtomicInteger(-1);

    private static Logger        log = Logger.getLogger(ProducerWeaverClusterTest.class.getCanonicalName());

    static {
        ClusterTestCfg.setTestPorts(24020, 24040, 24060, 24080);
    }

    public static void reset() {
        id.set(-1);
    }

    private ArrayList<AnnotationConfigApplicationContext> allContexts    = new ArrayList<AnnotationConfigApplicationContext>();
    private ClusterMaster                                 clusterMaster;
    private TestController                                controller;
    private final ArrayList<TestNode>                     fullPartition  = new ArrayList<TestNode>();
    private final BitView                                 fullView       = new BitView();
    private final ArrayList<ProducerCoordinator>          majorProducers = new ArrayList<ProducerCoordinator>();
    private final ArrayList<Producer>                     minorProducers = new ArrayList<Producer>();
    private final BitView                                 majorView      = new BitView();
    private final ArrayList<TestNode>                     majorViewNodes = new ArrayList<TestNode>();
    private final ArrayList<TestNode>                     minorViewNodes = new ArrayList<TestNode>();
    private final ArrayList<WeaverCoordinator>            majorWeavers   = new ArrayList<WeaverCoordinator>();
    private final ArrayList<Weaver>                       minorWeavers   = new ArrayList<Weaver>();
    private final ArrayList<Node>                         producerNodes  = new ArrayList<Node>();
    private final ArrayList<ProducerCoordinator>          producers      = new ArrayList<ProducerCoordinator>();
    private final MersenneTwister                         twister        = new MersenneTwister(
                                                                                               666);
    private final ArrayList<Node>                         weaverNodes    = new ArrayList<Node>();
    private final ArrayList<WeaverCoordinator>            weavers        = new ArrayList<WeaverCoordinator>();

    private ArrayList<Source>                             sources;
    private ArrayList<Source>                             majorSources;

    @Before
    public void startUp() throws Exception {
        ClusterTestCfg.incrementPorts();
        reset();
        log.info("Setting up initial partition");

        Class<?>[] weaverConfigs = weaverConfigurations();
        Class<?>[] producerConfigs = producerConfigurations();

        int numberOfMembers = weaverConfigs.length + producerConfigs.length + 1;
        CountDownLatch initialLatch = new CountDownLatch(numberOfMembers);
        AnnotationConfigApplicationContext controllerContext = new AnnotationConfigApplicationContext(
                                                                                                      ClusterControllerCfg.class);
        controller = controllerContext.getBean(TestController.class);
        controller.cardinality = numberOfMembers;
        controller.latch = initialLatch;
        AnnotationConfigApplicationContext clusterMasterContext = new AnnotationConfigApplicationContext(
                                                                                                         master.class);
        clusterMaster = clusterMasterContext.getBean(ClusterMaster.class);

        List<AnnotationConfigApplicationContext> weaverContexts = createContexts(weaverConfigs);
        List<AnnotationConfigApplicationContext> producerContexts = createContexts(producerConfigs);

        allContexts.add(clusterMasterContext);
        allContexts.addAll(weaverContexts);
        allContexts.addAll(producerContexts);
        allContexts.add(controllerContext);

        log.info("Awaiting initial partition stability");
        assertTrue("Initial partition did not acheive stability",
                   initialLatch.await(120, TimeUnit.SECONDS));
        log.info("Initial partition stable");

        Identity nodeId = clusterMasterContext.getBean(Identity.class);
        majorView.add(nodeId);
        fullView.add(nodeId);
        TestNode testNode = (TestNode) controller.getNode(nodeId);
        majorViewNodes.add(testNode);

        int i = 0;
        for (AnnotationConfigApplicationContext ctxt : weaverContexts) {
            WeaverCoordinator coordinator = ctxt.getBean(WeaverCoordinator.class);
            weavers.add(coordinator);
            nodeId = ctxt.getBean(Identity.class);
            testNode = (TestNode) controller.getNode(nodeId);
            weaverNodes.add(ctxt.getBean(Node.class));
            if (i < weavers.size() / 2 + 1) {
                majorView.add(nodeId);
                fullView.add(nodeId);
                majorViewNodes.add(testNode);
                majorWeavers.add(coordinator);
            } else {
                minorWeavers.add(coordinator.getWeaver());
                minorViewNodes.add(testNode);
            }
            i++;
        }
        ArrayList<AnnotationConfigApplicationContext> majorProducerContexts = new ArrayList<AnnotationConfigApplicationContext>();
        i = 0;
        for (AnnotationConfigApplicationContext ctxt : producerContexts) {
            nodeId = ctxt.getBean(Identity.class);
            testNode = (TestNode) controller.getNode(nodeId);
            producerNodes.add(ctxt.getBean(Node.class));
            ProducerCoordinator coordinator = ctxt.getBean(ProducerCoordinator.class);
            producers.add(coordinator);
            if (i < producers.size() / 2 + 1) {
                nodeId = ctxt.getBean(Identity.class);
                majorView.add(nodeId);
                fullView.add(nodeId);
                majorViewNodes.add(testNode);
                majorProducers.add(coordinator);
                majorProducerContexts.add(ctxt);
            } else {
                minorProducers.add(coordinator.getProducer());
                minorViewNodes.add(testNode);
            }
            i++;
        }
        log.info(String.format("Major partition: %s", majorViewNodes));

        sources = getSources(producerContexts);
        majorSources = getSources(majorProducerContexts);
    }

    @After
    public void tearDown() throws Exception {
        if (allContexts != null) {
            for (AnnotationConfigApplicationContext context : allContexts) {
                try {
                    context.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void testPartitioning() throws Exception {
        bootstrap();
        asymmetricallyPartition();
        reformPartition();
    }

    @Test
    public void testSimplePublishing() throws Exception {
        bootstrap();
        ConsistentHashFunction<Producer> producerRing = new ConsistentHashFunction<Producer>(
                                                                                             new ProducerSkipStrategy(),
                                                                                             producers.get(0).getProducer().createRing().replicaePerBucket);
        for (ProducerCoordinator producer : producers) {
            producerRing.add(producer.getProducer(), producer.getId().capacity);
        }
        List<UUID> channels = openChannels();
        Executor executor = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(sources.size());

        // Open the channels
        for (UUID channel : channels) {
            assertTrue(String.format("Channel not opened: %s", channel),
                       clusterMaster.open(channel, 60, TimeUnit.SECONDS));
        }

        for (Source source : sources) {
            source.publish(BATCH_COUNT, BATCH_SIZE, executor, latch, 0L);
        }

        assertTrue("not all publishers completed",
                   latch.await(60, TimeUnit.SECONDS));

        final Long target = Long.valueOf(BATCH_COUNT - 1);
        for (UUID channel : channels) {
            final UUID c = channel;
            final List<Producer> pair = producerRing.hash(point(channel), 2);
            waitFor(String.format("Did not receive all acks for %s", channel),
                    new Condition() {
                        @Override
                        public boolean value() {
                            Long ts = pair.get(1).getMirrorTimestampFor(c);
                            return target.equals(ts);
                        }
                    }, 60000L, 1000L);
        }
    }

    @Test
    public void testPublishingAfterPartition() throws Exception {
        bootstrap();
        ConsistentHashFunction<Producer> producerRing = new ConsistentHashFunction<Producer>(
                                                                                             new ProducerSkipStrategy(),
                                                                                             producers.get(0).getProducer().createRing().replicaePerBucket);
        for (ProducerCoordinator producer : producers) {
            producerRing.add(producer.getProducer(), producer.getId().capacity);
        }
        ConsistentHashFunction<Weaver> weaverRing = new ConsistentHashFunction<Weaver>(
                                                                                       new WeaverSkipStrategy(),
                                                                                       weavers.get(0).getWeaver().createRing().replicaePerBucket);
        for (WeaverCoordinator weaver : weavers) {
            weaverRing.add(weaver.getWeaver(), weaver.getId().capacity);
        }
        ArrayList<UUID> channels = openChannels();
        Executor executor = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(sources.size());

        // Open the channels
        for (UUID channel : channels) {
            assertTrue(String.format("Channel not opened: %s", channel),
                       clusterMaster.open(channel, 10, TimeUnit.SECONDS));
        }

        for (Source source : sources) {
            source.publish(BATCH_COUNT, BATCH_SIZE, executor, latch, 0L);
        }

        assertTrue("not all publishers completed",
                   latch.await(60, TimeUnit.SECONDS));

        final Long target = Long.valueOf(BATCH_COUNT - 1);
        for (UUID channel : channels) {
            final UUID c = channel;
            final List<Producer> pair = producerRing.hash(point(channel), 2);
            waitFor(String.format("Did not receive all acks for %s", channel),
                    new Condition() {
                        @Override
                        public boolean value() {
                            Long ts = pair.get(1).getMirrorTimestampFor(c);
                            return target.equals(ts);
                        }
                    }, 60000L, 1000L);
        }

        asymmetricallyPartition();
        reformPartition();

        latch = new CountDownLatch(majorSources.size());
        for (Source source : majorSources) {
            source.publish(BATCH_COUNT, BATCH_SIZE, executor, latch,
                           BATCH_COUNT);
        }

        assertTrue("not all publishers completed",
                   latch.await(60, TimeUnit.SECONDS));

        for (Source source : majorSources) {
            assertTrue(source.failedChannels.isEmpty());
        }
    }

    // @Test
    public void testPublishingDuringPartition() throws Exception {
        bootstrap();
        ConsistentHashFunction<Producer> producerRing = new ConsistentHashFunction<Producer>(
                                                                                             new ProducerSkipStrategy(),
                                                                                             producers.get(0).getProducer().createRing().replicaePerBucket);
        for (ProducerCoordinator producer : producers) {
            producerRing.add(producer.getProducer(), producer.getId().capacity);
        }
        ConsistentHashFunction<Weaver> weaverRing = new ConsistentHashFunction<Weaver>(
                                                                                       new WeaverSkipStrategy(),
                                                                                       weavers.get(0).getWeaver().createRing().replicaePerBucket);
        for (WeaverCoordinator weaver : weavers) {
            weaverRing.add(weaver.getWeaver(), weaver.getId().capacity);
        }
        ArrayList<UUID> channels = openChannels();
        Executor executor = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(majorSources.size());

        // Open the channels
        for (UUID channel : channels) {
            assertTrue(String.format("Channel not opened: %s", channel),
                       clusterMaster.open(channel, 10, TimeUnit.SECONDS));
        }

        int targetCount = BATCH_COUNT * 20;
        for (Source source : sources) {
            source.publish(targetCount, BATCH_SIZE, executor, latch, 0L);
        }

        asymmetricallyPartition();

        assertTrue("not all publishers completed",
                   latch.await(60, TimeUnit.SECONDS));

        ArrayList<UUID> lostChannels = filterChannelsByProducers(channels,
                                                                 producerRing);
        lostChannels.addAll(filterChannelsByWeavers(lostChannels, weaverRing));

        final Long target = Long.valueOf(targetCount - 1);
        for (UUID channel : channels) {
            final UUID c = channel;
            final List<Producer> pair = producerRing.hash(point(channel), 2);
            if (majorProducers.contains(pair.get(1))) {
                waitFor(String.format("Did not receive all acks for %s",
                                      channel), new Condition() {
                    @Override
                    public boolean value() {
                        Long ts = pair.get(1).getMirrorTimestampFor(c);
                        return target.equals(ts);
                    }
                }, 60000L, 1000L);
            }
        }

        for (Source source : majorSources) {
            if (!source.failedChannels.isEmpty()) {
                for (UUID channel : source.failedChannels) {
                    assertTrue(lostChannels.contains(channel));
                }
            }
        }
    }

    protected void assertProducersBootstrapping(List<ProducerCoordinator> partition)
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

    protected void assertProducersStable(List<ProducerCoordinator> partition)
                                                                             throws InterruptedException {
        for (ProducerCoordinator coordinator : partition) {
            final ProducerCoordinator c = coordinator;
            waitFor("Coordinator never entered the stable state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return ProducerCoordinatorContext.CoordinatorFSM.Stable == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected void assertWeaversBootstrapping(List<WeaverCoordinator> partition)
                                                                                throws InterruptedException {
        for (WeaverCoordinator coordinator : partition) {
            final WeaverCoordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return WeaverCoordinatorContext.CoordinatorFSM.Bootstrapping == c.getState()
                           || WeaverCoordinatorContext.BootstrapFSM.Bootstrap == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected void assertWeaversStable(List<WeaverCoordinator> partition)
                                                                         throws InterruptedException {
        for (WeaverCoordinator coordinator : partition) {
            final WeaverCoordinator c = coordinator;
            waitFor("Coordinator never entered the stable state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return WeaverCoordinatorContext.CoordinatorFSM.Stable == c.getState();
                }
            }, 120000, 1000);
        }
    }

    void assertProducersActive(List<ProducerCoordinator> partition)
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

    void assertWeaversActive(List<WeaverCoordinator> partition)
                                                               throws InterruptedException {
        for (WeaverCoordinator coordinator : partition) {
            final WeaverCoordinator c = coordinator;
            waitFor("Coordinator never entered the active state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.isActive();
                }
            }, 120000, 1000);
        }
    }

    void asymmetricallyPartition() throws InterruptedException {
        CountDownLatch latchA = latch(majorViewNodes);
        log.info("Asymmetrically partitioning");
        controller.asymPartition(majorView);

        log.info("Awaiting stability of major partition");
        assertTrue("major partition did not stabilize",
                   latchA.await(60, TimeUnit.SECONDS));

        log.info("Major partition has stabilized");

        // Check to see everything is as expected.
        for (TestNode member : majorViewNodes) {
            assertEquals(majorView, member.getPartition());
        }
        // Check to see if the major partition is stable.
        assertWeaversStable(majorWeavers);
        assertProducersStable(majorProducers);
    }

    void bootstrap() throws InterruptedException {
        assertWeaversBootstrapping(weavers);
        assertProducersBootstrapping(producers);
        log.info("Bootstrapping the weavers");
        weavers.get(weavers.size() - 1).initiateBootstrap();
        producers.get(producers.size() - 1).initiateBootstrap();

        assertWeaversStable(weavers);
        assertProducersStable(producers);
        assertProducersActive(producers);
        assertWeaversActive(weavers);
    }

    List<AnnotationConfigApplicationContext> createContexts(Class<?>[] configs) {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
    }

    CountDownLatch latch(List<TestNode> group) {
        CountDownLatch latch = new CountDownLatch(group.size());
        for (TestNode member : group) {
            member.latch = latch;
            member.cardinality = group.size();
        }
        return latch;
    }

    Class<?>[] producerConfigurations() {
        return new Class<?>[] { producer1.class, producer.class,
                producer.class, producer.class, producer.class, producer2.class };
    }

    void reformPartition() throws InterruptedException {
        // reform
        CountDownLatch latch = latch(fullPartition);

        // Clear the partition
        log.info("Reforming partition");
        controller.clearPartitions();

        log.info("Awaiting stability of reformed partition");
        assertTrue("Full partition did not stablize",
                   latch.await(60, TimeUnit.SECONDS));

        log.info("Full partition has stabilized");

        // Check to see everything is kosher
        for (TestNode member : fullPartition) {
            assertEquals(fullView, member.getPartition());
        }
        // Check to see if the participants are stable.
        assertWeaversStable(weavers);
        assertProducersStable(producers);
    }

    Class<?>[] weaverConfigurations() {
        return new Class<?>[] { weaver1.class, weaver.class, weaver.class,
                weaver.class, weaver.class, weaver2.class };
    }

    ArrayList<UUID> openChannels() throws InterruptedException {
        log.info("Creating some channels");

        int numOfChannels = CHANNEL_COUNT;
        ArrayList<UUID> channels = new ArrayList<UUID>();
        for (int i = 0; i < numOfChannels; i++) {
            UUID channel = new UUID(twister.nextLong(), twister.nextLong());
            channels.add(channel);
            log.info(String.format("Opening channel: %s", channel));
            assertTrue(String.format("Channel %s did not successfully open",
                                     channel),
                       clusterMaster.open(channel, 60, TimeUnit.SECONDS));
        }
        return channels;
    }

    ArrayList<Source> getSources(List<AnnotationConfigApplicationContext> contexts) {
        ArrayList<Source> sources = new ArrayList<Source>();
        for (BeanFactory f : contexts) {
            sources.add(f.getBean(Source.class));
        }
        return sources;
    }

    ArrayList<UUID> filterChannelsByWeavers(ArrayList<UUID> channels,
                                            ConsistentHashFunction<Weaver> ring) {
        ArrayList<UUID> lostChannels = new ArrayList<UUID>();
        for (UUID channel : channels) {
            if (minorWeavers.containsAll(ring.hash(point(channel), 2))) {
                lostChannels.add(channel);
            }
        }
        channels.removeAll(lostChannels);
        return lostChannels;
    }

    ArrayList<UUID> filterChannelsByProducers(ArrayList<UUID> channels,
                                              ConsistentHashFunction<Producer> ring) {
        ArrayList<UUID> lostChannels = new ArrayList<UUID>();
        for (UUID channel : channels) {
            if (minorProducers.containsAll(ring.hash(point(channel), 2))) {
                lostChannels.add(channel);
            }
        }
        channels.removeAll(lostChannels);
        return lostChannels;
    }
}
