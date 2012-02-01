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

import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.views.BitView;
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

/**
 * @author hhildebrand
 * 
 */
public class ProducerWeaverClusterTest {
    private static Logger        log = Logger.getLogger(ProducerWeaverClusterTest.class.getCanonicalName());
    private static AtomicInteger id  = new AtomicInteger(-1);

    public static void reset() {
        id.set(-1);
    }

    static {
        ClusterTestCfg.setTestPorts(24020, 24040, 24060, 24080);
    }

    @Configuration
    static class master extends ClusterNodeCfg {

        private int node = -1;

        @Bean
        @Autowired
        public ClusterMaster clusterMaster(Switchboard switchboard) {
            return new ClusterMaster(switchboard);
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

        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    @Configuration
    @Import(ProducerCfg.class)
    static class producer extends ClusterNodeCfg {

        private int node = -1;

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

        public int node() {
            if (node == -1) {
                node = id.incrementAndGet();
            }
            return node;
        }
    }

    private ClusterMaster                                 clusterMaster;
    private TestController                                controller;
    private ArrayList<AnnotationConfigApplicationContext> allContexts;
    private ArrayList<AnnotationConfigApplicationContext> weaverContexts;
    private ArrayList<TestNode>                           fullPartition;
    private BitView                                       majorView;

    private Class<?>[] weaverConfigurations() {
        return new Class<?>[] { weaver1.class, weaver.class, weaver.class,
                weaver.class, weaver2.class };
    }

    private Class<?>[] producerConfigurations() {
        return new Class<?>[] { producer1.class, producer.class,
                producer.class, producer.class, producer2.class };
    }

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

        allContexts = new ArrayList<AnnotationConfigApplicationContext>();
        allContexts.add(clusterMasterContext);
        allContexts.addAll(weaverContexts);
        allContexts.addAll(producerContexts);
        allContexts.add(controllerContext);

        log.info("Awaiting initial partition stability");
        assertTrue("Initial partition did not acheive stability",
                   initialLatch.await(120, TimeUnit.SECONDS));
        log.info("Initial partition stable");
        fullPartition = new ArrayList<TestNode>();
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

    private List<AnnotationConfigApplicationContext> createContexts(Class<?>[] configs) {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
    }

    @Test
    public void testPartitioning() {

    }
}
