package com.salesforce.ouroboros.producer.functional.util;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;

@Configuration
@Import({ nodeCfg.class })
public class weaver0 extends GossipDiscoveryNode1Cfg {
    @Override
    public int node() {
        return 3;
    }
}