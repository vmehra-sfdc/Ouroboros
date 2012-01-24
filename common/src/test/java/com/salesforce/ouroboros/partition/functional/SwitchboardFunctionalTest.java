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
package com.salesforce.ouroboros.partition.functional;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.salesforce.ouroboros.partition.functional.util.M;
import com.salesforce.ouroboros.partition.functional.util.MyControllerConfig;
import com.salesforce.ouroboros.partition.functional.util.node0;
import com.salesforce.ouroboros.partition.functional.util.node1;
import com.salesforce.ouroboros.partition.functional.util.node10;
import com.salesforce.ouroboros.partition.functional.util.node11;
import com.salesforce.ouroboros.partition.functional.util.node12;
import com.salesforce.ouroboros.partition.functional.util.node13;
import com.salesforce.ouroboros.partition.functional.util.node14;
import com.salesforce.ouroboros.partition.functional.util.node15;
import com.salesforce.ouroboros.partition.functional.util.node16;
import com.salesforce.ouroboros.partition.functional.util.node17;
import com.salesforce.ouroboros.partition.functional.util.node18;
import com.salesforce.ouroboros.partition.functional.util.node19;
import com.salesforce.ouroboros.partition.functional.util.node2;
import com.salesforce.ouroboros.partition.functional.util.node3;
import com.salesforce.ouroboros.partition.functional.util.node4;
import com.salesforce.ouroboros.partition.functional.util.node5;
import com.salesforce.ouroboros.partition.functional.util.node6;
import com.salesforce.ouroboros.partition.functional.util.node7;
import com.salesforce.ouroboros.partition.functional.util.node8;
import com.salesforce.ouroboros.partition.functional.util.node9;
import com.salesforce.ouroboros.partition.functional.util.nodeCfg;
import com.salesforce.ouroboros.testUtils.ControlNode;
import com.salesforce.ouroboros.testUtils.PartitionController;
import com.salesforce.ouroboros.testUtils.Util;
import com.salesforce.ouroboros.testUtils.Util.Condition;

/**
 * 
 * @author hhildebrand
 * 
 */
public class SwitchboardFunctionalTest {
    private static final Logger              log     = Logger.getLogger(SwitchboardFunctionalTest.class.getCanonicalName());

    final Class<?>[]                         configs = getConfigs();
    PartitionController                      controller;
    AnnotationConfigApplicationContext       controllerContext;
    CountDownLatch                           initialLatch;
    List<AnnotationConfigApplicationContext> memberContexts;
    List<ControlNode>                        partition;

    @Before
    public void setUp() throws Exception {
        nodeCfg.testPort1++;
        nodeCfg.testPort2++;
        log.info("Setting up initial partition");
        initialLatch = new CountDownLatch(configs.length);
        controllerContext = new AnnotationConfigApplicationContext(
                                                                   getControllerConfig());
        controller = (PartitionController) controllerContext.getBean(Controller.class);
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
            Util.waitFor(String.format("Member %s is not stable", m.node),
                         new Condition() {
                             @Override
                             public boolean value() {
                                 return member.stabilized;
                             }
                         }, 60000, 100);
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
