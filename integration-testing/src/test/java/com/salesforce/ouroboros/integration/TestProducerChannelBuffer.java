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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.testUtil.TestController;
import com.hellblazer.jackal.testUtil.TestNode;
import com.hellblazer.jackal.testUtil.gossip.GossipControllerCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;
import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode2Cfg;
import com.hellblazer.jackal.testUtil.gossip.GossipTestCfg;
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
    static class ProducerCfg extends GossipDiscoveryNode1Cfg {

        @Autowired
        private Partition partitionManager;

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

        @Bean
        public Switchboard switchboard() {
            Switchboard switchboard = new Switchboard(
                                                      memberNode(),
                                                      partitionManager,
                                                      Generators.timeBasedGenerator());
            return switchboard;
        }

        @Bean
        public Node memberNode() {
            return new Node(node(), node(), node());
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
    }

    @Configuration
    static class WeaverCfg extends GossipDiscoveryNode2Cfg {
        @Autowired
        private Partition partitionManager;

        @Bean
        public com.salesforce.ouroboros.spindle.Coordinator coordinator()
                                                                         throws IOException {
            return new com.salesforce.ouroboros.spindle.Coordinator(
                                                                    timer(),
                                                                    switchboard(),
                                                                    weaver());
        }

        @Bean
        public Switchboard switchboard() {
            Switchboard switchboard = new Switchboard(
                                                      memberNode(),
                                                      partitionManager,
                                                      Generators.timeBasedGenerator());
            return switchboard;
        }

        @Bean
        public Node memberNode() {
            return new Node(node(), node(), node());
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

    private static final Logger        log = Logger.getLogger(TestProducerChannelBuffer.class.getCanonicalName());
    static {
        GossipTestCfg.setTestPorts(24020, 24040);
    }

    TestController                     controller;
    AnnotationConfigApplicationContext controllerContext;
    CountDownLatch                     initialLatch;
    List<TestNode>                     partition;
    AnnotationConfigApplicationContext producerContext;
    AnnotationConfigApplicationContext weaverContext;

    @Before
    public void starUp() throws Exception {
        GossipTestCfg.incrementPorts();
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(2);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   GossipControllerCfg.class);
        controller = controllerContext.getBean(TestController.class);
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
            partition = new ArrayList<TestNode>();
            TestNode member = (TestNode) controller.getNode(producerContext.getBean(Identity.class));
            assertNotNull("Can't find node: "
                                  + producerContext.getBean(Identity.class),
                          member);
            partition.add(member);
            member = (TestNode) controller.getNode(weaverContext.getBean(Identity.class));
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
