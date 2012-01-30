package com.salesforce.ouroboros.partition.functional;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;

@Configuration
@Import({ nodeCfg.class })
public class member extends GossipNodeCfg {
    final static AtomicInteger id = new AtomicInteger(-1);

    static void reset() {
        id.set(-1);
    }

    @Override
    @Bean
    public int node() {
        return id.incrementAndGet();
    }
}