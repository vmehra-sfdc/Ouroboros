package com.salesforce.ouroboros.producer.functional.util;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode2Cfg;

@Configuration
@Import({ nodeCfg.class })
public class weaver2 extends GossipDiscoveryNode2Cfg {
    @Override
    public int node() {
        return 12;
    }
}