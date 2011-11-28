/*               
 * Copyright (C) 2008-2010 Paolo Boldi, Massimo Santini and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

package com.salesforce.ouroboros.partition;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.BasicConfiguration;
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
import com.salesforce.ouroboros.ChannelMessage;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.RebalanceMessage;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.Util.Condition;

/**
 * 
 * @author hhildebrand
 * 
 */
public class SwitchboardFunctionalTest {
    public static class ControlNode extends NodeData {
        static final Logger log = Logger.getLogger(ControlNode.class.getCanonicalName());

        int                 cardinality;
        CountDownLatch      latch;

        public ControlNode(Heartbeat hb, Controller controller) {
            super(hb, controller);
        }

        @Override
        protected void partitionNotification(View partition, int leader) {
            log.fine("Partition notification: " + partition);
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

    static class M implements Member {
        final Node        node;
        volatile boolean  stabilized = false;
        final Switchboard switchboard;

        M(Node n, Switchboard s) {
            node = n;
            switchboard = s;
            switchboard.setMember(this);
        }

        @Override
        public void advertise() {
            System.err.println("advertising");
            switchboard.ringCast(new Message(
                                             node,
                                             GlobalMessageType.ADVERTISE_CONSUMER));
        }

        @Override
        public void destabilize() {
            stabilized = false;
        }

        @Override
        public void dispatch(ChannelMessage type, Node sender,
                             Serializable[] arguments, long time) {

        }

        @Override
        public void dispatch(FailoverMessage type, Node sender,
                             Serializable[] arguments, long time) {
        }

        @Override
        public void dispatch(GlobalMessageType type,
                             com.salesforce.ouroboros.Node sender,
                             Serializable[] arguments, long time) {

        }

        @Override
        public void dispatch(MemberDispatch type,
                             com.salesforce.ouroboros.Node sender,
                             Serializable[] arguments, long time) {

        }

        @Override
        public void dispatch(RebalanceMessage type, Node sender,
                             Serializable[] arguments, long time) {

        }

        @Override
        public void stabilized() {
            stabilized = true;
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
        protected Controller constructController() throws UnknownHostException {
            return new MyController(timer(), 1000, 300000, partitionIdentity(),
                                    heartbeatTimeout(), heartbeatInterval());
        }

    }

    @Configuration
    static class node0 extends nodeCfg {
        @Override
        public int node() {
            return 0;
        }
    }

    @Configuration
    static class node1 extends nodeCfg {
        @Override
        public int node() {
            return 1;
        }
    }

    @Configuration
    static class node10 extends nodeCfg {
        @Override
        public int node() {
            return 10;
        }
    }

    @Configuration
    static class node11 extends nodeCfg {
        @Override
        public int node() {
            return 11;
        }
    }

    @Configuration
    static class node12 extends nodeCfg {
        @Override
        public int node() {
            return 12;
        }
    }

    @Configuration
    static class node13 extends nodeCfg {
        @Override
        public int node() {
            return 13;
        }
    }

    @Configuration
    static class node14 extends nodeCfg {
        @Override
        public int node() {
            return 14;
        }
    }

    @Configuration
    static class node15 extends nodeCfg {
        @Override
        public int node() {
            return 15;
        }
    }

    @Configuration
    static class node16 extends nodeCfg {
        @Override
        public int node() {
            return 16;
        }
    }

    @Configuration
    static class node17 extends nodeCfg {
        @Override
        public int node() {
            return 17;
        }
    }

    @Configuration
    static class node18 extends nodeCfg {
        @Override
        public int node() {
            return 18;
        }
    }

    @Configuration
    static class node19 extends nodeCfg {
        @Override
        public int node() {
            return 19;
        }
    }

    @Configuration
    static class node2 extends nodeCfg {
        @Override
        public int node() {
            return 2;
        }
    }

    @Configuration
    static class node3 extends nodeCfg {
        @Override
        public int node() {
            return 3;
        }
    }

    @Configuration
    static class node4 extends nodeCfg {
        @Override
        public int node() {
            return 4;
        }
    }

    @Configuration
    static class node5 extends nodeCfg {
        @Override
        public int node() {
            return 5;
        }
    }

    @Configuration
    static class node6 extends nodeCfg {
        @Override
        public int node() {
            return 6;
        }
    }

    @Configuration
    static class node7 extends nodeCfg {
        @Override
        public int node() {
            return 7;
        }
    }

    @Configuration
    static class node8 extends nodeCfg {
        @Override
        public int node() {
            return 8;
        }
    }

    @Configuration
    static class node9 extends nodeCfg {
        @Override
        public int node() {
            return 9;
        }
    }

    static class nodeCfg extends BasicConfiguration {
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

        @Bean
        public M member() {
            return new M(memberNode(), switchboard());
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
    }

    private static final Logger              log     = Logger.getLogger(SwitchboardFunctionalTest.class.getCanonicalName());

    final Class<?>[]                         configs = getConfigs();
    MyController                             controller;
    AnnotationConfigApplicationContext       controllerContext;
    CountDownLatch                           initialLatch;
    List<AnnotationConfigApplicationContext> memberContexts;
    List<ControlNode>                        partition;

    @Before
    public void setUp() throws Exception {
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(configs.length);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   getControllerConfig());
        controller = (MyController) controllerContext.getBean(Controller.class);
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
            for (AnnotationConfigApplicationContext context : memberContexts) {
                ControlNode member = (ControlNode) controller.getNode(context.getBean(Identity.class));
                assertNotNull("Can't find node: "
                                      + context.getBean(Identity.class), member);
                partition.add(member);
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

    @Test
    public void testDiscovery() throws Exception {
        List<M> vnodes = new ArrayList<M>();
        for (AnnotationConfigApplicationContext context : memberContexts) {
            vnodes.add(context.getBean(M.class));
        }
        assertEquals(memberContexts.size(), vnodes.size());
        for (M m : vnodes) {
            final M member = m;
            Util.waitFor("Member is not stable", new Condition() {

                @Override
                public boolean value() {
                    return member.stabilized;
                }
            }, 10000, 100);
        }
    }

    protected Class<?>[] getConfigs() {
        return new Class[] { node0.class, node1.class, node2.class,
                node3.class, node4.class, node5.class, node6.class,
                node7.class, node8.class, node9.class, node10.class,
                node11.class, node12.class, node13.class, node14.class,
                node15.class, node16.class, node17.class, node18.class,
                node19.class };
    }

    protected Class<?> getControllerConfig() {
        return MyControllerConfig.class;
    }

    private List<AnnotationConfigApplicationContext> createMembers() {
        ArrayList<AnnotationConfigApplicationContext> contexts = new ArrayList<AnnotationConfigApplicationContext>();
        for (Class<?> config : configs) {
            contexts.add(new AnnotationConfigApplicationContext(config));
        }
        return contexts;
    }
}
