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

import static com.salesforce.ouroboros.producer.Util.waitFor;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
import com.hellblazer.jackal.annotations.DeployedPostProcessor;
import com.hellblazer.jackal.gossip.configuration.ControllerGossipConfiguration;
import com.hellblazer.jackal.gossip.configuration.GossipConfiguration;
import com.hellblazer.pinkie.SocketOptions;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.producer.CoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.producer.CoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.producer.Util.Condition;
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

    @Configuration
    static class MyControllerConfig extends ControllerGossipConfiguration {

        @Override
        @Bean
        public DeployedPostProcessor deployedPostProcessor() {
            return new DeployedPostProcessor();
        }

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

        @Bean(initMethod = "start", destroyMethod = "terminate")
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
    static class w0 extends nodeCfg {
        @Override
        public int node() {
            return 0;
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact1();
        }
    }

    @Configuration
    static class w1 extends nodeCfg {
        @Override
        public int node() {
            return 1;
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

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact2();
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

    private final Class<?>[]                         configs = new Class[] {
            w0.class, w1.class, w2.class, w3.class, w4.class, w5.class,
            w6.class, w7.class, w8.class, w9.class          };
    private MyController                             controller;
    private AnnotationConfigApplicationContext       controllerContext;
    private List<Coordinator>                        coordinators;
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
    @SuppressWarnings("unused")
    private MersenneTwister                          twister = new MersenneTwister(
                                                                                   666);

    @Before
    public void startUp() throws Exception {
        testPort1++;
        testPort2++;
        log.info("Setting up initial partition");
        CountDownLatch initialLatch = new CountDownLatch(configs.length);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   MyControllerConfig.class);
        controller = controllerContext.getBean(MyController.class);
        controller.cardinality = configs.length;
        controller.latch = initialLatch;
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

        CountDownLatch latchA = latch(majorGroup);
        // CountDownLatch latchB = latch(minorGroup);

        log.info("Bootstrapping the producers");
        coordinators.get(coordinators.size() - 1).initiateBootstrap();

        assertPartitionStable(coordinators);
        assertPartitionActive(coordinators);

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

        // major partition should be active and awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        assertPartitionActive(majorPartition);

        log.info("Failing over");
        Switchboard leader = memberContexts.get(0).getBean(Switchboard.class);
        leader.ringCast(new Message(leader.getId(), FailoverMessage.PREPARE));
        leader.ringCast(new Message(leader.getId(), FailoverMessage.FAILOVER));

        // major partition should be stable and active
        assertPartitionStable(majorPartition);
        assertPartitionActive(majorPartition);

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

        // Major partition should be awaiting failover
        assertPartitionAwaitingFailover(majorPartition);
        leader.ringCast(new Message(leader.getId(), FailoverMessage.PREPARE));
        leader.ringCast(new Message(leader.getId(), FailoverMessage.FAILOVER));

        // Only the major partition should be active 
        assertPartitionActive(majorPartition);
        assertPartitionInactive(minorPartition);
        
        // Entire partition should be stable
        assertPartitionStable(coordinators);
    }

    private List<AnnotationConfigApplicationContext> createMembers() {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
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
}
