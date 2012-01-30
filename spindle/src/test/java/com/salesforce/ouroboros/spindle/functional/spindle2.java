package com.salesforce.ouroboros.spindle.functional;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode2Cfg;

@Configuration
@Import({ nodeCfg.class })
public class spindle2 extends GossipDiscoveryNode2Cfg {
    private int                node = -1;

    @Override
    public int node() {
        if (node == -1) {
            node = spindle.id.incrementAndGet();
        }
        return node;
    }
}