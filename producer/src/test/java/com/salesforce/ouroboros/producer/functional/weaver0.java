package com.salesforce.ouroboros.producer.functional;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;

@Configuration
@Import({ nodeCfg.class })
public class weaver0 extends GossipDiscoveryNode1Cfg {
    private int node = -1;

    @Override
    public int node() {
        if (node == -1) {
            node = weaver.id.incrementAndGet();
        }
        return node;
    }
}