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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.WeaverConfigation;
import com.salesforce.ouroboros.spindle.WeaverCoordinator;

/**
 * 
 * @author hhildebrand
 * 
 */
@Configuration
public class WeaverCfg {

    @Bean
    public WeaverCoordinator coordinator(Switchboard switchboard, Weaver weaver)
                                                                                throws IOException {
        return new WeaverCoordinator(timer(), switchboard, weaver);
    }

    @Bean
    @Autowired
    public Node memberNode(Identity partitionIdentity) {
        return new Node(partitionIdentity.id);
    }

    @Bean
    public File rootDirectory() throws IOException {
        File directory = File.createTempFile("prod-CB", ".root");
        directory.delete();
        directory.mkdirs();
        directory.deleteOnExit();
        return directory;
    }

    @Bean
    public Switchboard switchboard(Node memberNode, Partition partitionManager) {
        Switchboard switchboard = new Switchboard(
                                                  memberNode,
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Bean
    public ScheduledExecutorService timer() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    public Weaver weaver(WeaverConfigation weaverConfiguration)
                                                               throws IOException {
        return new Weaver(weaverConfiguration);
    }

    @Bean
    @Autowired
    public WeaverConfigation weaverConfiguration(Node memberNode)
                                                                 throws IOException {
        File directory = rootDirectory();
        WeaverConfigation weaverConfigation = new WeaverConfigation();
        weaverConfigation.setId(memberNode);
        weaverConfigation.addRoot(directory);
        return weaverConfigation;
    }
}