package com.salesforce.ouroboros.spindle.functional.util;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;

@Configuration
@Import({ nodeCfg.class })
public class spindle1 extends GossipDiscoveryNode1Cfg {
    @Override
    public int node() {
        return 1;
    }
}