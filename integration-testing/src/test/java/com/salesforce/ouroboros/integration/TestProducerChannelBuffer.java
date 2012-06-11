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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.TestController;
import com.hellblazer.jackal.testUtil.TestNode;
import com.hellblazer.jackal.testUtil.TestNodeCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipControllerCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;
import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode2Cfg;
import com.hellblazer.jackal.testUtil.gossip.GossipTestCfg;
import com.salesforce.ouroboros.endpoint.EndpointCoordinatorContext;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardFSM;
import com.salesforce.ouroboros.producer.ProducerCoordinator;
import com.salesforce.ouroboros.spindle.WeaverCoordinator;
import com.salesforce.ouroboros.spindle.WeaverCoordinatorContext;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestProducerChannelBuffer {
    @Configuration
    @Import(WeaverCfg.class)
    static class cbConfig extends GossipDiscoveryNode2Cfg {

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
    static class pCfg extends GossipDiscoveryNode1Cfg {

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

    private static final Logger  log = LoggerFactory.getLogger(TestProducerChannelBuffer.class.getCanonicalName());

    static {
        GossipTestCfg.setTestPorts(24120, 24140);
    }

    public static void reset() {
        id.set(-1);
    }

    TestController                     controller;
    AnnotationConfigApplicationContext controllerContext;
    CountDownLatch                     initialLatch;
    List<TestNode>                     partition;
    AnnotationConfigApplicationContext producerContext;
    AnnotationConfigApplicationContext weaverContext;

    @Before
    public void starUp() throws Exception {
        TestNodeCfg.nextMagic();
        GossipTestCfg.incrementPorts();
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(2);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   GossipControllerCfg.class);
        controller = controllerContext.getBean(TestController.class);
        controller.cardinality = 2;
        controller.latch = initialLatch;
        producerContext = new AnnotationConfigApplicationContext(pCfg.class);
        weaverContext = new AnnotationConfigApplicationContext(cbConfig.class);
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
        log.info("TestProducerChannelBuffer.testPush");
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

        final ProducerCoordinator producer = producerContext.getBean(ProducerCoordinator.class);
        final WeaverCoordinator weaver = weaverContext.getBean(WeaverCoordinator.class);

        weaver.initiateBootstrap();

        Util.waitFor("weaver coordinator did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return weaver.getState() == WeaverCoordinatorContext.CoordinatorFSM.Stable;
                         }
                     }, 30000, 200);

        producer.initiateBootstrap();

        Util.waitFor("producer coordinator did not stabilize",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             return producer.getState() == EndpointCoordinatorContext.CoordinatorFSM.Stable;
                         }
                     }, 30000, 200);
    }
}
