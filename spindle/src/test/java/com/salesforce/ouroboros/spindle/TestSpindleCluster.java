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

import static com.salesforce.ouroboros.spindle.Util.waitFor;
import static com.salesforce.ouroboros.util.Utils.point;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.views.BitView;
import org.smartfrog.services.anubis.partition.views.View;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;
import org.smartfrog.services.anubis.partition.wire.security.WireSecurity;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.gossip.configuration.ControllerGossipConfiguration;
import com.hellblazer.jackal.gossip.configuration.GossipConfiguration;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;
import com.salesforce.ouroboros.spindle.CoordinatorContext.BootstrapFSM;
import com.salesforce.ouroboros.spindle.CoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.spindle.Util.Condition;
import com.salesforce.ouroboros.spindle.source.Spindle;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.MersenneTwister;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpindleCluster {
    private static final int  MAGIC         = 666;
    private static final Node PRODUCER_NODE = new Node(666);

    public static class ControlNode extends NodeData {
        int            cardinality;
        CountDownLatch latch;

        public ControlNode(Heartbeat hb, Controller controller) {
            super(hb, controller);
        }

        @Override
        protected void partitionNotification(View partition, int leader) {
            log.finest("Partition notification: " + partition);
            super.partitionNotification(partition, leader);
            if (partition.isStable() && partition.cardinality() == cardinality) {
                latch.countDown();
            }
        }
    }

    public static class MyController extends Controller {
        int            cardinality;
        CountDownLatch latch;

        public MyController(Timer timer, long checkPeriod, long expirePeriod,
                            Identity partitionIdentity, long heartbeatTimeout,
                            long heartbeatInterval,
                            SocketOptions socketOptions,
                            Executor dispatchExecutor, WireSecurity wireSecurity)
                                                                                 throws IOException {
            super(timer, checkPeriod, expirePeriod, partitionIdentity,
                  heartbeatTimeout, heartbeatInterval, socketOptions,
                  dispatchExecutor, wireSecurity);
        }

        @Override
        protected NodeData createNode(Heartbeat hb) {
            ControlNode node = new ControlNode(hb, this);
            node.cardinality = cardinality;
            node.latch = latch;
            return node;
        }

    }

    private static class ClusterMaster implements Member {
        CountDownLatch    openLatch;
        final Switchboard switchboard;

        public ClusterMaster(Switchboard switchboard) {
            this.switchboard = switchboard;
            switchboard.setMember(this);
        }

        @Override
        public void advertise() {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             DiscoveryMessage.ADVERTISE_NOOP));
        }

        @Override
        public void becomeInactive() {
        }

        @Override
        public void destabilize() {
        }

        @Override
        public void dispatch(BootstrapMessage type, Node sender,
                             Serializable[] arguments, long time) {
        }

        @Override
        public void dispatch(ChannelMessage type, Node sender,
                             Serializable[] arguments, long time) {
            if (openLatch == null) {
                return;
            }
            switch (type) {
                case PRIMARY_OPENED:
                case MIRROR_OPENED:
                    openLatch.countDown();
                    break;
                default:
            }
        }

        @Override
        public void dispatch(DiscoveryMessage type, Node sender,
                             Serializable[] arguments, long time) {
        }

        @Override
        public void dispatch(FailoverMessage type, Node sender,
                             Serializable[] arguments, long time) {
        }

        @Override
        public void dispatch(WeaverRebalanceMessage type, Node sender,
                             Serializable[] arguments, long time) {
        }

        public boolean open(UUID channel, long timeout, TimeUnit unit)
                                                                      throws InterruptedException {
            openLatch = new CountDownLatch(2);
            switchboard.ringCast(new Message(switchboard.getId(),
                                             ChannelMessage.OPEN,
                                             new Serializable[] { channel }));
            return openLatch.await(timeout, unit);
        }

        @Override
        public void stabilized() {
        }

    }

    @Configuration
    static class ClusterMasterCfg extends GossipConfiguration {
        @Bean
        public ClusterMaster clusterMaster() {
            return new ClusterMaster(switchboard());
        }

        @Override
        public int getMagic() {
            try {
                return Identity.getMagicFromLocalIpAddress();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        @Bean
        public AnubisLocator locator() {
            return null;
        }

        @Bean
        public Node memberNode() {
            return new Node(node(), node(), node());
        }

        @Override
        public int node() {
            return 0;
        }

        @Bean
        public Switchboard switchboard() {
            Switchboard switchboard = new Switchboard(
                                                      memberNode(),
                                                      partition(),
                                                      Generators.timeBasedGenerator());
            return switchboard;
        }

        @Bean
        public ScheduledExecutorService timer() {
            return Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        protected Collection<InetSocketAddress> seedHosts()
                                                           throws UnknownHostException {
            return asList(seedContact1(), seedContact2());
        }

        InetSocketAddress seedContact1() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort1);
        }

        InetSocketAddress seedContact2() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort2);
        }
    }

    @Configuration
    static class MyControllerConfig extends ControllerGossipConfiguration {

        @Override
        public int magic() {
            try {
                return Identity.getMagicFromLocalIpAddress();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        protected Controller constructController() throws IOException {
            return new MyController(timer(), 1000, 300000, partitionIdentity(),
                                    heartbeatTimeout(), heartbeatInterval(),
                                    socketOptions(), dispatchExecutor(),
                                    wireSecurity());
        }

        @Override
        protected Collection<InetSocketAddress> seedHosts()
                                                           throws UnknownHostException {
            return asList(seedContact1(), seedContact2());
        }

        InetSocketAddress seedContact1() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort1);
        }

        InetSocketAddress seedContact2() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort2);
        }
    }

    static class nodeCfg extends GossipConfiguration {

        @Bean
        public Coordinator coordinator() throws IOException {
            return new Coordinator(timer(), switchboard(), weaver());
        }

        @Override
        public int getMagic() {
            try {
                return Identity.getMagicFromLocalIpAddress();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        @Bean
        public AnubisLocator locator() {
            return null;
        }

        @Bean
        public Node memberNode() {
            return new Node(node(), node(), node());
        }

        @Bean
        public Switchboard switchboard() {
            Switchboard switchboard = new Switchboard(
                                                      memberNode(),
                                                      partition(),
                                                      Generators.timeBasedGenerator());
            return switchboard;
        }

        @Bean
        public ScheduledExecutorService timer() {
            return Executors.newSingleThreadScheduledExecutor();
        }

        @Bean
        public Weaver weaver() throws IOException {
            return new Weaver(weaverConfiguration());
        }

        private WeaverConfigation weaverConfiguration() throws IOException {
            File directory = File.createTempFile("CoordinatorIntegration-",
                                                 "root");
            directory.delete();
            directory.mkdirs();
            directory.deleteOnExit();
            WeaverConfigation weaverConfigation = new WeaverConfigation();
            weaverConfigation.setId(memberNode());
            weaverConfigation.addRoot(directory);
            weaverConfigation.setMaxSegmentSize(MAX_SEGMENT_SIZE);
            return weaverConfigation;
        }

        @Override
        protected Collection<InetSocketAddress> seedHosts()
                                                           throws UnknownHostException {
            return asList(seedContact1(), seedContact2());
        }

        InetSocketAddress seedContact1() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort1);
        }

        InetSocketAddress seedContact2() throws UnknownHostException {
            return new InetSocketAddress("127.0.0.1", testPort2);
        }
    }

    @Configuration
    static class w1 extends nodeCfg {
        @Override
        public int node() {
            return 1;
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact1();
        }
    }

    @Configuration
    static class w10 extends nodeCfg {
        @Override
        public int node() {
            return 10;
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact2();
        }
    }

    @Configuration
    static class w2 extends nodeCfg {
        @Override
        public int node() {
            return 2;
        }
    }

    @Configuration
    static class w3 extends nodeCfg {
        @Override
        public int node() {
            return 3;
        }
    }

    @Configuration
    static class w4 extends nodeCfg {
        @Override
        public int node() {
            return 4;
        }
    }

    @Configuration
    static class w5 extends nodeCfg {
        @Override
        public int node() {
            return 5;
        }
    }

    @Configuration
    static class w6 extends nodeCfg {
        @Override
        public int node() {
            return 6;
        }
    }

    @Configuration
    static class w7 extends nodeCfg {
        @Override
        public int node() {
            return 7;
        }
    }

    @Configuration
    static class w8 extends nodeCfg {
        @Override
        public int node() {
            return 8;
        }
    }

    @Configuration
    static class w9 extends nodeCfg {
        @Override
        public int node() {
            return 9;
        }
    }

    private static final Logger                      log              = Logger.getLogger(TestSpindleCluster.class.getCanonicalName());
    private static final int                         MAX_SEGMENT_SIZE = 1024 * 1024;
    private static int                               testPort1;
    private static int                               testPort2;

    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "24506");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "24320");
        testPort2 = Integer.parseInt(port);
    }

    private ClusterMaster                            clusterMaster;
    private AnnotationConfigApplicationContext       clusterMasterContext;
    private final Class<?>[]                         configs          = new Class[] {
            w1.class, w2.class, w3.class, w4.class, w5.class, w6.class,
            w7.class, w8.class, w9.class, w10.class                  };
    private MyController                             controller;
    private AnnotationConfigApplicationContext       controllerContext;
    private List<Coordinator>                        coordinators;
    private List<ControlNode>                        fullPartition;
    private List<Node>                               fullPartitionId;
    private BitView                                  fullView;
    private List<ControlNode>                        majorGroup;
    private List<Coordinator>                        majorPartition;
    private List<Node>                               majorPartitionId;
    private BitView                                  majorView;
    private List<Weaver>                             majorWeavers;
    private List<AnnotationConfigApplicationContext> memberContexts;
    private List<ControlNode>                        minorGroup;
    private List<Coordinator>                        minorPartition;
    private List<Node>                               minorPartitionId;
    private BitView                                  minorView;
    private List<Weaver>                             minorWeavers;
    private MersenneTwister                          twister          = new MersenneTwister(
                                                                                            666);
    private List<Weaver>                             weavers;

    @Before
    public void startUp() throws Exception {
        testPort1++;
        testPort2++;
        log.info("Setting up initial partition");
        CountDownLatch initialLatch = new CountDownLatch(configs.length + 1);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   MyControllerConfig.class);
        controller = controllerContext.getBean(MyController.class);
        controller.cardinality = configs.length + 1;
        controller.latch = initialLatch;
        clusterMasterContext = new AnnotationConfigApplicationContext(
                                                                      ClusterMasterCfg.class);
        clusterMaster = clusterMasterContext.getBean(ClusterMaster.class);
        memberContexts = createMembers();
        fullPartition = new ArrayList<ControlNode>();
        coordinators = new ArrayList<Coordinator>();
        weavers = new ArrayList<Weaver>();
        log.info("Awaiting initial partition stability");
        boolean success = false;
        try {
            success = initialLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Initial partition did not acheive stability", success);
            log.info("Initial partition stable");
            fullPartitionId = new ArrayList<Node>();
            for (AnnotationConfigApplicationContext context : memberContexts) {
                ControlNode node = (ControlNode) controller.getNode(context.getBean(Identity.class));
                assertNotNull("Can't find node: "
                                      + context.getBean(Identity.class), node);
                fullPartition.add(node);
                Coordinator coordinator = context.getBean(Coordinator.class);
                fullPartitionId.add(coordinator.getId());
                assertNotNull("Can't find coordinator in context: " + context,
                              coordinator);
                coordinators.add(coordinator);
                weavers.add(context.getBean(Weaver.class));
            }
        } finally {
            if (!success) {
                tearDown();
            }
        }
        ControlNode clusterMasterNode = (ControlNode) controller.getNode(clusterMasterContext.getBean(Identity.class));
        fullPartition.add(clusterMasterNode);

        assertPartitionBootstrapping(coordinators);

        majorView = new BitView();
        minorView = new BitView();
        fullView = new BitView();

        majorPartition = new ArrayList<Coordinator>();
        minorPartition = new ArrayList<Coordinator>();

        majorGroup = new ArrayList<ControlNode>();
        minorGroup = new ArrayList<ControlNode>();

        majorPartitionId = new ArrayList<Node>();
        minorPartitionId = new ArrayList<Node>();

        majorWeavers = new ArrayList<Weaver>();
        minorWeavers = new ArrayList<Weaver>();

        int majorPartitionSize = coordinators.size() / 2 + 1;

        // Form the major partition
        for (int i = 0; i < majorPartitionSize; i++) {
            ControlNode member = fullPartition.get(i);
            Coordinator coordinator = coordinators.get(i);
            majorPartitionId.add(fullPartitionId.get(i));
            majorPartition.add(coordinator);
            fullView.add(member.getIdentity());
            majorGroup.add(member);
            majorView.add(member.getIdentity());
            majorWeavers.add(weavers.get(i));
        }
        majorGroup.add(clusterMasterNode);
        fullView.add(clusterMasterNode.getIdentity());
        majorView.add(clusterMasterNode.getIdentity());

        // Form the minor partition
        for (int i = majorPartitionSize; i < coordinators.size(); i++) {
            ControlNode member = fullPartition.get(i);
            Coordinator coordinator = coordinators.get(i);
            minorPartitionId.add(coordinator.getId());
            minorPartition.add(coordinator);
            fullView.add(member.getIdentity());
            minorGroup.add(member);
            minorView.add(member.getIdentity());
            minorWeavers.add(weavers.get(i));
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

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

        // The other partition should still be unstable.
        // assertEquals(minorGroup.size(), latchB.getCount());

        reformPartition();

        // Entire partition should be stable
        assertPartitionStable(coordinators);

        // Only the major partition should be stable
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);
    }

    /**
     * Test the rebalancing of the system upon partitioning and reformation.
     * Bootstrap the cluster, create a number of channels. Partition the cluster
     * and ensure that cluster stabilizes and fails over correctly. Reform the
     * partion and ensure that the entire cluster is stable, but on the major
     * partition is active. Activate the minor partition members, ensuring that
     * the cluster rebalances and stabilizes.
     * 
     * @throws Exception
     */
    @Test
    public void testRebalancing() throws Exception {

        bootstrap();

        ArrayList<UUID> channels = openChannels();

        // Construct the hash ring that maps the channels to nodes
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        for (Node node : fullPartitionId) {
            ring.add(node, node.capacity);
        }

        assertChannelMappings(channels, weavers, ring);

        asymmetricallyPartition();

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

        // Filter out all the channels with a primary and secondary on the minor partition
        List<UUID> lostChannels = filterChannels(channels, ring);

        assertFailoverChannelMappings(channels, majorPartitionId, majorWeavers,
                                      ring);

        validateLostChannels(lostChannels, majorWeavers);

        // Rebalance the open channels across the major partition
        log.info("Initiating rebalance on majority partition");
        Coordinator partitionLeader = majorPartition.get(majorPartition.size() - 1);
        assertTrue("coordinator is not the leader",
                   partitionLeader.isActiveLeader());
        partitionLeader.initiateRebalance();

        assertPartitionStable(majorPartition);

        reformPartition();

        // Entire partition should be stable
        assertPartitionStable(coordinators);

        // Only the major partition should be stable
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);

        ring = new ConsistentHashFunction<Node>();
        for (Node node : majorPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, majorWeavers, ring);

        validateLostChannels(lostChannels, majorWeavers);

        log.info("Activating minor partition");
        // Now add the minor partition back into the active set
        ArrayList<Node> minorPartitionNodes = new ArrayList<Node>();
        for (Coordinator c : minorPartition) {
            minorPartitionNodes.add(c.getId());
        }
        partitionLeader.initiateRebalance(minorPartitionNodes.toArray(new Node[0]));

        // Entire partition should be stable 
        assertPartitionStable(coordinators);

        // Entire partition should be active
        assertPartitionActive(coordinators);

        // Everything should be back to normal
        ring = new ConsistentHashFunction<Node>();
        for (Node node : fullPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, weavers, ring);

        // validate lost channels aren't mapped on the partition members
        validateLostChannels(lostChannels, weavers);
    }

    /**
     * Test the replication of state within the system. Create a cluster and
     * open some channels. Create some segment data on the primaries and ensure
     * that it has been replicated to the mirrors. Partition the system and
     * ensure that the channel state is where it should be. Rebalance the active
     * members and ensure that all the state has been xeroxed and is where it
     * should be. Rebalance the group with the inactive members and ensure that
     * all the state has been xeroxed to where it should be.
     * 
     * @throws Exception
     */
    // @Test
    public void testReplication() throws Exception {

        bootstrap();

        ArrayList<UUID> channels = openChannels();

        // Construct the hash ring that maps the channels to nodes
        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        for (Node node : fullPartitionId) {
            ring.add(node, node.capacity);
        }

        assertChannelMappings(channels, weavers, ring);

        for (UUID channel : channels) {
            createSegments(ring, channel);
        }

        asymmetricallyPartition();

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

        // Filter out all the channels with a primary and secondary on the minor partition
        List<UUID> lostChannels = filterChannels(channels, ring);

        assertFailoverChannelMappings(channels, majorPartitionId, majorWeavers,
                                      ring);

        validateLostChannels(lostChannels, majorWeavers);

        // Rebalance the open channels across the major partition
        log.info("Initiating rebalance on majority partition");
        Coordinator partitionLeader = majorPartition.get(majorPartition.size() - 1);
        assertTrue("coordinator is not the leader",
                   partitionLeader.isActiveLeader());
        partitionLeader.initiateRebalance();

        assertPartitionStable(majorPartition);

        reformPartition();

        // Entire partition should be stable
        assertPartitionStable(coordinators);

        // Only the major partition should be stable
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);

        ring = new ConsistentHashFunction<Node>();
        for (Node node : majorPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, majorWeavers, ring);

        validateLostChannels(lostChannels, majorWeavers);

        log.info("Activating minor partition");
        // Now add the minor partition back into the active set
        ArrayList<Node> minorPartitionNodes = new ArrayList<Node>();
        for (Coordinator c : minorPartition) {
            minorPartitionNodes.add(c.getId());
        }
        partitionLeader.initiateRebalance(minorPartitionNodes.toArray(new Node[0]));

        // Entire partition should be stable 
        assertPartitionStable(coordinators);

        // Entire partition should be active
        assertPartitionActive(coordinators);

        // Everything should be back to normal
        ring = new ConsistentHashFunction<Node>();
        for (Node node : fullPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, weavers, ring);

        // validate lost channels aren't mapped on the partition members
        validateLostChannels(lostChannels, weavers);
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
        for (ControlNode member : majorGroup) {
            assertEquals(majorView, member.getPartition());
        }
    }

    private void bootstrap() throws InterruptedException {
        log.info("Bootstrapping the spindles");
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

    private ArrayList<UUID> openChannels() throws InterruptedException {
        log.info("Creating some channels");

        int numOfChannels = 100;
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

    private void reformPartition() throws InterruptedException {
        // reform
        CountDownLatch latch = new CountDownLatch(fullPartition.size());
        for (ControlNode node : fullPartition) {
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

        // Check to see everything is kosher
        for (ControlNode member : fullPartition) {
            assertEquals(fullView, member.getPartition());
        }
    }

    private void validateLostChannels(List<UUID> lostChannels,
                                      List<Weaver> weavers) {
        for (UUID channel : lostChannels) {
            for (Weaver weaver : weavers) {
                assertNull(String.format("%s should not be hosting lost channel %s",
                                         weaver.getId(), channel),
                           weaver.eventChannelFor(channel));
            }
        }
    }

    protected void assertChannelMappings(List<UUID> channels,
                                         List<Weaver> weaverGroup,
                                         ConsistentHashFunction<Node> ring) {
        for (UUID channel : channels) {
            List<Node> mapping = ring.hash(point(channel), 2);
            Node primary = mapping.get(0);
            Node mirror = mapping.get(1);
            for (Weaver weaver : weaverGroup) {
                if (primary.equals(weaver.getId())) {
                    EventChannel eventChannel = weaver.eventChannelFor(channel);
                    assertNotNull(String.format("%s should be the primary for %s, but doesn't host it",
                                                primary, channel), eventChannel);
                    assertTrue(String.format("%s should be the primary for %s, but is mirror",
                                             primary, channel),
                               eventChannel.isPrimary());
                } else {
                    if (mirror.equals(weaver.getId())) {
                        EventChannel eventChannel = weaver.eventChannelFor(channel);
                        assertNotNull(String.format("%s should be the mirror for %s, but doesn't host it",
                                                    mirror, channel),
                                      eventChannel);
                        assertTrue(String.format("%s should be the mirror for %s, but is primary",
                                                 mirror, channel),
                                   eventChannel.isMirror());
                    } else {
                        assertNull(String.format("%s claims to host %s, but isn't marked as primary or mirror",
                                                 weaver.getId(), channel),
                                   weaver.eventChannelFor(channel));
                    }
                }
            }
        }
    }

    protected void assertFailoverChannelMappings(List<UUID> channels,
                                                 List<Node> idGroup,
                                                 List<Weaver> weaverGroup,
                                                 ConsistentHashFunction<Node> ring) {
        for (UUID channel : channels) {
            List<Node> mapping = ring.hash(point(channel), 2);
            Node primary = mapping.get(0);
            Node mirror = mapping.get(1);
            for (Weaver weaver : weaverGroup) {
                if (!idGroup.contains(primary)) {
                    if (mirror.equals(weaver.getId())) {
                        EventChannel eventChannel = weaver.eventChannelFor(channel);
                        assertNotNull(String.format("%s should be the failed over primary for %s, but doesn't host it",
                                                    mirror, channel),
                                      eventChannel);
                        assertTrue(String.format("%s should be the failed over primary for %s, but is mirror",
                                                 mirror, channel),
                                   eventChannel.isPrimary());
                    }
                } else if (!idGroup.contains(mirror)) {
                    if (primary.equals(weaver.getId())) {
                        EventChannel eventChannel = weaver.eventChannelFor(channel);
                        assertNotNull(String.format("%s should remain the primary for %s, but doesn't host it",
                                                    primary, channel),
                                      eventChannel);
                        assertTrue(String.format("%s should remain the primary for %s, but is mirror",
                                                 primary, channel),
                                   eventChannel.isPrimary());
                    }
                } else {
                    if (primary.equals(weaver.getId())) {
                        EventChannel eventChannel = weaver.eventChannelFor(channel);
                        assertNotNull(String.format("%s should be the primary for %s, but doesn't host it",
                                                    primary, channel),
                                      eventChannel);
                        assertTrue(String.format("%s should be the primary for %s, but is mirror",
                                                 primary, channel),
                                   eventChannel.isPrimary());
                    } else {
                        if (mirror.equals(weaver.getId())) {
                            EventChannel eventChannel = weaver.eventChannelFor(channel);
                            assertNotNull(String.format("%s should be the mirror for %s, but doesn't host it",
                                                        mirror, channel),
                                          eventChannel);
                            assertTrue(String.format("%s should be the mirror for %s, but is primary",
                                                     mirror, channel),
                                       eventChannel.isMirror());
                        } else {
                            assertNull(String.format("%s claims to host %s, but isn't marked as primary or mirror",
                                                     weaver.getId(), channel),
                                       weaver.eventChannelFor(channel));
                        }
                    }
                }
            }
        }
    }

    protected void assertPartitionActive(List<Coordinator> partition)
                                                                     throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.isActive();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionBootstrapping(List<Coordinator> partition)
                                                                            throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return CoordinatorFSM.Bootstrapping == c.getState()
                           || BootstrapFSM.Bootstrap == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionInactive(List<Coordinator> partition)
                                                                       throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return !c.isActive();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionStable(List<Coordinator> partition)
                                                                     throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the stable state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return CoordinatorFSM.Stable == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected CountDownLatch latch(List<ControlNode> group) {
        CountDownLatch latch = new CountDownLatch(group.size());
        for (ControlNode member : group) {
            member.latch = latch;
            member.cardinality = group.size();
        }
        return latch;
    }

    protected void createSegments(ConsistentHashFunction<Node> ring,
                                  UUID channel) throws IOException {
        Node node = ring.hash(point(channel));
        assertNotNull(node);
        Weaver weaver = weavers.get(node.processId - 1);
        assertEquals(node, weaver.getId());
        log.info(String.format("Creating segments for %s on %s", channel,
                               weaver.getId()));
        SocketOptions options = new SocketOptions();
        SocketChannel outbound = SocketChannel.open();
        options.setTimeout(1000);
        options.configure(outbound.socket());
        outbound.configureBlocking(true);
        outbound.connect(weaver.getContactInformation().spindle);
        ByteBuffer buffer = ByteBuffer.allocate(Spindle.HANDSHAKE_SIZE);
        buffer.putInt(Spindle.MAGIC);
        PRODUCER_NODE.serialize(buffer);
        buffer.flip();
        outbound.write(buffer);
        byte[] payload = payloadFor(channel);
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event event = new Event(MAGIC, payloadBuffer);
        int batchSize = 10;
        int batchLength = batchSize * event.totalSize();
        int totalSize = 1024 * 16;
        long timestamp = 0;
        for (int i = 0; i < totalSize; i += batchLength) {
            BatchHeader header = new BatchHeader(PRODUCER_NODE, batchLength,
                                                 MAGIC, channel, timestamp);
            timestamp += batchSize;
            header.rewind();
            header.write(outbound);
            for (int j = 0; j < batchSize; j++) {
                event.rewind();
                event.write(outbound);
            }
        }
        // ByteBuffer ackBuffer = ByteBuffer.allocate(BatchIdentity.BYTE_SIZE);
        // outbound.read(ackBuffer);
        // outbound.close();
    }

    private byte[] payloadFor(UUID channel) {
        return String.format("%s Give me Slack, or give me Food, or Kill me %s",
                             channel, channel).getBytes();
    }
}
