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
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.BasicConfiguration;
import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.ControllerConfiguration;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.views.View;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hellblazer.jackal.annotations.DeployedPostProcessor;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.spindle.CoordinatorContext.ControllerFSM;
import com.salesforce.ouroboros.spindle.CoordinatorContext.CoordinatorFSM;
import com.salesforce.ouroboros.spindle.Util.Condition;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestCluster {

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

        public MyController(Timer timer, long checkPeriod, long expirePeriod,
                            Identity partitionIdentity, long heartbeatTimeout,
                            long heartbeatInterval) {
            super(timer, checkPeriod, expirePeriod, partitionIdentity,
                  heartbeatTimeout, heartbeatInterval);
        }

        @Override
        protected NodeData createNode(Heartbeat hb) {
            ControlNode node = new ControlNode(hb, this);
            node.cardinality = cardinality;
            node.latch = latch;
            return node;
        }

    }

    @Configuration
    static class MyControllerConfig extends ControllerConfiguration {
        @Override
        @Bean
        public DeployedPostProcessor deployedPostProcessor() {
            return new DeployedPostProcessor();
        }

        @Override
        public int heartbeatGroupTTL() {
            return 0;
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
        protected MyController constructController()
                                                    throws UnknownHostException {
            return new MyController(timer(), 1000, 300000, partitionIdentity(),
                                    heartbeatTimeout(), heartbeatInterval());
        }

    }

    static class nodeCfg extends BasicConfiguration {
        @Bean
        public Coordinator coordinator() throws IOException {
            return new Coordinator(timer(), switchboard(), weaver(),
                                   new CoordinatorConfiguration());
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
        public int heartbeatGroupTTL() {
            return 0;
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

        @Bean(initMethod = "start", destroyMethod = "terminate")
        public Switchboard switchboard() {
            Switchboard switchboard = new Switchboard(memberNode(), partition());
            return switchboard;
        }

        @Bean
        public ScheduledExecutorService timer() {
            return Executors.newSingleThreadScheduledExecutor();
        }

        @Bean(initMethod = "start", destroyMethod = "terminate")
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
            return weaverConfigation;
        }
    }

    @Configuration
    static class w0 extends nodeCfg {
        @Override
        public int node() {
            return 0;
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
    }

    private static final Logger              log     = Logger.getLogger(TestCluster.class.getCanonicalName());

    final Class<?>[]                         configs = new Class[] { w0.class,
                    w1.class, w2.class, w3.class, w4.class, w5.class, w6.class,
                    w7.class, w8.class, w9.class    };

    MyController                             controller;
    AnnotationConfigApplicationContext       controllerContext;
    CountDownLatch                           initialLatch;
    List<AnnotationConfigApplicationContext> memberContexts;
    List<ControlNode>                        partition;
    List<Coordinator>                        coordinators;

    @Before
    public void starUp() throws Exception {
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(configs.length);
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
            partition = new ArrayList<ControlNode>();
            coordinators = new ArrayList<Coordinator>();
            for (AnnotationConfigApplicationContext context : memberContexts) {
                ControlNode node = (ControlNode) controller.getNode(context.getBean(Identity.class));
                assertNotNull("Can't find node: "
                                              + context.getBean(Identity.class),
                              node);
                partition.add(node);
                Coordinator coordinator = context.getBean(Coordinator.class);
                assertNotNull("Can't find coordinator in context: " + context,
                              coordinator);
                coordinators.add(coordinator);
            }
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
        partition = null;
        initialLatch = null;
    }

    private List<AnnotationConfigApplicationContext> createMembers() {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
    }

    @Test
    public void testPartition() throws Exception {
        for (Coordinator coordinator : coordinators) {
            final Coordinator c = coordinator;
            waitFor("Coordinator never entered the bootstrapping state: "
                    + coordinator, new Condition() {
                @Override
                public boolean value() {
                    return CoordinatorFSM.Bootstrapping == c.getState()
                           || ControllerFSM.Bootstrap == c.getState();
                }
            }, 2000, 100);
        }
    }
}
