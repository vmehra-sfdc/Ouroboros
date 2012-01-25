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

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardFSM;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerConfiguration;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.WeaverConfigation;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestProducerChannelBuffer {

    public static class ControlNode extends NodeData {
        static final Logger log = Logger.getLogger(ControlNode.class.getCanonicalName());

        int                 cardinality;
        CountDownLatch      latch;

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

        public MyController(Identity partitionIdentity, int heartbeatTimeout,
                            int heartbeatInterval, SocketOptions socketOptions,
                            ExecutorService dispatchExecutor,
                            WireSecurity wireSecurity) throws IOException {
            super(partitionIdentity, heartbeatTimeout, heartbeatInterval,
                  socketOptions, dispatchExecutor, wireSecurity);
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
        public int magic() {
            try {
                return Identity.getMagicFromLocalIpAddress();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        protected Controller constructController() throws IOException {
            return new MyController(partitionIdentity(), heartbeatTimeout(),
                                    heartbeatInterval(), socketOptions(),
                                    dispatchExecutor(), wireSecurity());
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
    static class ProducerCfg extends nodeCfg {

        @Bean
        public ProducerConfiguration configuration() {
            return new ProducerConfiguration();
        }

        @Bean
        public com.salesforce.ouroboros.producer.Coordinator coordinator()
                                                                          throws IOException {
            return new com.salesforce.ouroboros.producer.Coordinator(
                                                                     switchboard(),
                                                                     producer());
        }

        /* (non-Javadoc)
         * @see org.smartfrog.services.anubis.BasicConfiguration#node()
         */
        @Override
        public int node() {
            return 2;
        }

        @Bean
        public Producer producer() throws IOException {
            return new Producer(memberNode(), source(), configuration());
        }

        @Bean
        public Source source() {
            return new Source();
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact1();
        }
    }

    @Configuration
    static class WeaverCfg extends nodeCfg {

        @Bean
        public com.salesforce.ouroboros.spindle.Coordinator coordinator()
                                                                         throws IOException {
            return new com.salesforce.ouroboros.spindle.Coordinator(
                                                                    timer(),
                                                                    switchboard(),
                                                                    weaver());
        }

        /* (non-Javadoc)
         * @see org.smartfrog.services.anubis.BasicConfiguration#node()
         */
        @Override
        public int node() {
            return 3;
        }

        @Bean
        public ScheduledExecutorService timer() {
            return Executors.newSingleThreadScheduledExecutor();
        }

        @Bean
        public Weaver weaver() throws IOException {
            return new Weaver(weaverConfiguration());
        }

        @Override
        protected InetSocketAddress gossipEndpoint()
                                                    throws UnknownHostException {
            return seedContact2();
        }

        private WeaverConfigation weaverConfiguration() throws IOException {
            File directory = rootDirectory();
            WeaverConfigation weaverConfigation = new WeaverConfigation();
            weaverConfigation.setId(memberNode());
            weaverConfigation.addRoot(directory);
            return weaverConfigation;
        }

        @Bean
        public File rootDirectory() throws IOException {
            File directory = File.createTempFile("prod-CB", ".root");
            directory.delete();
            directory.mkdirs();
            directory.deleteOnExit();
            return directory;
        }
    }

    static int                         testPort1;
    static int                         testPort2;
    private static final Logger        log = Logger.getLogger(TestProducerChannelBuffer.class.getCanonicalName());
    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "24020");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "24040");
        testPort2 = Integer.parseInt(port);
    }

    MyController                       controller;
    AnnotationConfigApplicationContext controllerContext;
    CountDownLatch                     initialLatch;
    List<ControlNode>                  partition;
    AnnotationConfigApplicationContext producerContext;
    AnnotationConfigApplicationContext weaverContext;

    @Before
    public void starUp() throws Exception {
        testPort1++;
        testPort2++;
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(2);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   MyControllerConfig.class);
        controller = controllerContext.getBean(MyController.class);
        controller.cardinality = 2;
        controller.latch = initialLatch;
        producerContext = new AnnotationConfigApplicationContext(
                                                                 ProducerCfg.class);
        weaverContext = new AnnotationConfigApplicationContext(WeaverCfg.class);
        log.info("Awaiting initial partition stability");
        boolean success = false;
        try {
            success = initialLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Initial partition did not acheive stability", success);
            log.info("Initial partition stable");
            partition = new ArrayList<ControlNode>();
            ControlNode member = (ControlNode) controller.getNode(producerContext.getBean(Identity.class));
            assertNotNull("Can't find node: "
                                  + producerContext.getBean(Identity.class),
                          member);
            partition.add(member);
            member = (ControlNode) controller.getNode(weaverContext.getBean(Identity.class));
            assertNotNull("Can't find node: "
                                  + weaverContext.getBean(Identity.class),
                          member);
            partition.add(member);
        } finally {
            if (!success) {
                tearDown();
            }
        }
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
        if (producerContext != null) {
            try {
                producerContext.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        if (weaverContext != null) {
            Utils.deleteDirectory(weaverContext.getBean(File.class));
            try {
                weaverContext.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        controller = null;
        partition = null;
        initialLatch = null;
    }

    @Test
    public void testPush() throws Exception {
        final Switchboard producerSwitchboard = producerContext.getBean(Switchboard.class);
        final Switchboard weaverSwitchboard = weaverContext.getBean(Switchboard.class);
        Util.waitFor("producer switchboard did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return producerSwitchboard.getState() == SwitchboardFSM.Stable;
                         }
                     }, 30000, 200);

        Util.waitFor("weaver switchboard did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return weaverSwitchboard.getState() == SwitchboardFSM.Stable;
                         }
                     }, 30000, 200);

        final com.salesforce.ouroboros.producer.Coordinator producer = producerContext.getBean(com.salesforce.ouroboros.producer.Coordinator.class);
        final com.salesforce.ouroboros.spindle.Coordinator weaver = weaverContext.getBean(com.salesforce.ouroboros.spindle.Coordinator.class);

        weaver.initiateBootstrap();

        Util.waitFor("weaver coordinator did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return weaver.getState() == com.salesforce.ouroboros.spindle.CoordinatorContext.CoordinatorFSM.Stable;
                         }
                     }, 30000, 200);

        producer.initiateBootstrap();

        Util.waitFor("producer coordinator did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return producer.getState() == com.salesforce.ouroboros.producer.CoordinatorContext.CoordinatorFSM.Stable;
                         }
                     }, 30000, 200);
    }
}
