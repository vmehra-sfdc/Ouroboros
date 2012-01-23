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

import static com.salesforce.ouroboros.testUtils.Util.waitFor;
import static com.salesforce.ouroboros.util.Utils.point;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;
import com.salesforce.ouroboros.producer.CoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.producer.CoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.testUtils.Util.Condition;
import com.salesforce.ouroboros.util.ConsistentHashFunction;
import com.salesforce.ouroboros.util.MersenneTwister;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestProducerCluster {

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

    public static class Source implements EventSource {

        @Override
        public void assumePrimary(Map<UUID, Long> newPrimaries) {
        }

        @Override
        public void closed(UUID channel) {
        }

        @Override
        public void opened(UUID channel) {
        }

    }

    private static class ClusterMaster implements Member {
        Semaphore                      semaphore = new Semaphore(0);
        final Switchboard              switchboard;
        final ScheduledExecutorService timer     = Executors.newSingleThreadScheduledExecutor();

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
            switch (type) {
                case BOOTSTRAP_SPINDLES:
                    semaphore.release();
                    break;
                default:
                    break;

            }
        }

        @Override
        public void dispatch(ChannelMessage type, Node sender,
                             Serializable[] arguments, long time) {
            semaphore.release();
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

        public void failover() {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             FailoverMessage.FAILOVER));
        }

        public boolean mirrorOpened(UUID channel, long timeout, TimeUnit unit)
                                                                              throws InterruptedException {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             ChannelMessage.MIRROR_OPENED,
                                             new Serializable[] { channel }));
            return acquire(timeout, unit).get();
        }

        public boolean open(UUID channel, long timeout, TimeUnit unit)
                                                                      throws InterruptedException {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             ChannelMessage.OPEN,
                                             new Serializable[] { channel }));
            return acquire(timeout, unit).get();
        }

        public void prepare() {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             FailoverMessage.PREPARE));
        }

        public boolean primaryOpened(UUID channel, long timeout, TimeUnit unit)
                                                                               throws InterruptedException {
            switchboard.ringCast(new Message(switchboard.getId(),
                                             ChannelMessage.PRIMARY_OPENED,
                                             new Serializable[] { channel }));
            return acquire(timeout, unit).get();
        }

        public boolean bootstrapSpindles(Node[] spindles, long timeout,
                                         TimeUnit unit)
                                                       throws InterruptedException {
            switchboard.ringCast(new Message(
                                             switchboard.getId(),
                                             BootstrapMessage.BOOTSTRAP_SPINDLES,
                                             (Serializable) spindles));
            return acquire(timeout, unit).get();
        }

        @Override
        public void stabilized() {
        }

        private AtomicBoolean acquire(long timeout, TimeUnit unit)
                                                                  throws InterruptedException {
            final AtomicBoolean acquired = new AtomicBoolean(true);
            timer.schedule(new Runnable() {
                @Override
                public void run() {
                    acquired.set(false);
                    semaphore.release();
                }
            }, timeout, unit);
            semaphore.acquire();
            return acquired;
        }

    }

    private static class FakeSpindle implements Member {
        int               PORT = 55555;
        final Switchboard switchboard;

        public FakeSpindle(Switchboard switchboard) {
            this.switchboard = switchboard;
            switchboard.setMember(this);
        }

        @Override
        public void advertise() {
            ContactInformation info = new ContactInformation(
                                                             new InetSocketAddress(
                                                                                   "127.0.0.1",
                                                                                   PORT++),
                                                             null, null);
            switchboard.ringCast(new Message(
                                             switchboard.getId(),
                                             DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER,
                                             info, true));
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
    static class f1 extends FakeSpindleCfg {
        @Override
        public int node() {
            return 1;
        }
    }

    @Configuration
    static class f2 extends FakeSpindleCfg {
        @Override
        public int node() {
            return 2;
        }
    }

    static class FakeSpindleCfg extends GossipConfiguration {
        @Bean
        public FakeSpindle fakeSpindle() {
            return new FakeSpindle(switchboard());
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
            return new Coordinator(switchboard(), producer());
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
        public Producer producer() throws IOException {
            return new Producer(memberNode(), source(), configuration());
        }

        @Bean
        public EventSource source() {
            return new Source();
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

        protected ProducerConfiguration configuration() {
            return new ProducerConfiguration();
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
    static class w10 extends nodeCfg {
        @Override
        public int node() {
            return 10;
        }
    }

    @Configuration
    static class w11 extends nodeCfg {
        @Override
        public int node() {
            return 11;
        }
    }

    @Configuration
    static class w12 extends nodeCfg {
        @Override
        public int node() {
            return 12;
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact2();
        }
    }

    @Configuration
    static class w3 extends nodeCfg {
        @Override
        public int node() {
            return 3;
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact1();
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

    private static final Logger                      log     = Logger.getLogger(TestProducerCluster.class.getCanonicalName());
    private static int                               testPort1;
    private static int                               testPort2;

    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "25230");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "25370");
        testPort2 = Integer.parseInt(port);
    }

    private ClusterMaster                            clusterMaster;
    private AnnotationConfigApplicationContext       clusterMasterContext;
    private final Class<?>[]                         configs = new Class[] {
            w3.class, w4.class, w5.class, w6.class, w7.class, w8.class,
            w9.class, w10.class, w11.class, w12.class       };
    private MyController                             controller;
    private AnnotationConfigApplicationContext       controllerContext;
    private List<Coordinator>                        coordinators;
    private List<AnnotationConfigApplicationContext> fakeSpindleContexts;
    private Node[]                                   fakeSpindleNodes;
    private List<ControlNode>                        fullPartition;
    private List<Node>                               fullPartitionId;
    private BitView                                  fullView;
    private List<ControlNode>                        majorGroup;
    private List<Coordinator>                        majorPartition;
    private List<Node>                               majorPartitionId;
    private List<Producer>                           majorProducers;
    private BitView                                  majorView;
    private List<AnnotationConfigApplicationContext> memberContexts;
    private List<ControlNode>                        minorGroup;
    private List<Coordinator>                        minorPartition;
    private List<Node>                               minorPartitionId;
    private List<Producer>                           minorProducers;
    private BitView                                  minorView;
    private List<Producer>                           producers;
    private MersenneTwister                          twister = new MersenneTwister(
                                                                                   666);

    @Before
    public void startUp() throws Exception {
        testPort1++;
        testPort2++;
        log.info("Setting up initial partition");
        CountDownLatch initialLatch = new CountDownLatch(configs.length + 3);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   MyControllerConfig.class);
        controller = controllerContext.getBean(MyController.class);
        controller.cardinality = configs.length + 3;
        controller.latch = initialLatch;
        clusterMasterContext = new AnnotationConfigApplicationContext(
                                                                      ClusterMasterCfg.class);
        clusterMaster = clusterMasterContext.getBean(ClusterMaster.class);
        fakeSpindleContexts = new ArrayList<AnnotationConfigApplicationContext>();
        fakeSpindleContexts.add(new AnnotationConfigApplicationContext(f1.class));
        fakeSpindleContexts.add(new AnnotationConfigApplicationContext(f2.class));
        memberContexts = createMembers();
        log.info("Awaiting initial partition stability");
        boolean success = false;
        try {
            success = initialLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Initial partition did not acheive stability", success);
            log.info("Initial partition stable");
            fullPartition = new ArrayList<ControlNode>();
            coordinators = new ArrayList<Coordinator>();
            producers = new ArrayList<Producer>();
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
                producers.add(context.getBean(Producer.class));
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

        ArrayList<Node> nodes = new ArrayList<Node>();
        for (AnnotationConfigApplicationContext ctxt : fakeSpindleContexts) {
            Identity id = ctxt.getBean(Identity.class);
            fullPartition.add((ControlNode) controller.getNode(id));
            fullView.add(id);
            majorView.add(id);
            nodes.add(ctxt.getBean(Node.class));
        }
        fakeSpindleNodes = nodes.toArray(new Node[nodes.size()]);

        majorPartition = new ArrayList<Coordinator>();
        minorPartition = new ArrayList<Coordinator>();

        majorGroup = new ArrayList<ControlNode>();
        minorGroup = new ArrayList<ControlNode>();

        majorPartitionId = new ArrayList<Node>();
        minorPartitionId = new ArrayList<Node>();

        majorProducers = new ArrayList<Producer>();
        minorProducers = new ArrayList<Producer>();

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
            majorProducers.add(producers.get(i));
        }

        majorGroup.add(clusterMasterNode);
        fullView.add(clusterMasterNode.getIdentity());
        majorView.add(clusterMasterNode.getIdentity());
        for (AnnotationConfigApplicationContext ctxt : fakeSpindleContexts) {
            Identity id = ctxt.getBean(Identity.class);
            majorGroup.add((ControlNode) controller.getNode(id));
        }

        // Form the minor partition
        for (int i = majorPartitionSize; i < coordinators.size(); i++) {
            ControlNode member = fullPartition.get(i);
            Coordinator coordinator = coordinators.get(i);
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

        ConsistentHashFunction<Node> ring = new ConsistentHashFunction<Node>();
        for (Producer p : producers) {
            Node n = p.getId();
            ring.add(n, n.capacity);
        }

        // Open the channels
        for (UUID channel : channels) {
            assertTrue("Channel OPEN message not received",
                       clusterMaster.open(channel, 60, TimeUnit.SECONDS));
            assertTrue("Channel OPEN_PRIMARY message not received",
                       clusterMaster.primaryOpened(channel, 60,
                                                   TimeUnit.SECONDS));
            assertTrue("Channel OPEN_MIRROR message not received",
                       clusterMaster.mirrorOpened(channel, 60, TimeUnit.SECONDS));
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
        Coordinator partitionLeader = majorPartition.get(majorPartition.size() - 1);
        assertTrue("coordinator is not the leader",
                   partitionLeader.isActiveLeader());
        partitionLeader.initiateRebalance();

        assertPartitionStable(majorPartition);

        reformPartition();

        // Check to see everything is kosher
        for (ControlNode member : fullPartition) {
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

        ring = new ConsistentHashFunction<Node>();
        for (Node node : majorPartitionId) {
            ring.add(node, node.capacity);
        }
        assertChannelMappings(channels, majorProducers, ring);

        validateLostChannels(lostChannels, majorProducers);

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
        assertChannelMappings(channels, producers, ring);

        // validate lost channels aren't mapped on the partition members
        validateLostChannels(lostChannels, producers);
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
        for (ControlNode member : fullPartition) {
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
    }

    protected void assertPartitionActive(List<Coordinator> partition)
                                                                     throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the active state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.isActive();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionAwaitingFailover(List<Coordinator> partition)
                                                                               throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the awaiting failover state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return c.getState() == CoordinatorFSM.AwaitingFailover;
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
                           || ControllerFSM.Bootstrap == c.getState();
                }
            }, 120000, 1000);
        }
    }

    protected void assertPartitionInactive(List<Coordinator> partition)
                                                                       throws InterruptedException {
        for (Coordinator coordinator : partition) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the inactive state: "
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
                        assertTrue(String.format("%s should be the mirror for %s, but doesn't host it",
                                                 mirror, channel),
                                   producer.isActingMirrorFor(channel));
                        assertFalse(String.format("%s should be the mirror for %s, but is primary",
                                                  mirror, channel),
                                    producer.isActingPrimaryFor(channel));
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
}
