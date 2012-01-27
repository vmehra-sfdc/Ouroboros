package com.salesforce.ouroboros.producer.functional.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;

@Configuration
@Import({ nodeCfg.class })
public class weaver extends GossipNodeCfg {
    private static final AtomicInteger id = new AtomicInteger(3);
    
    public static void reset() {
        id.set(3);
    }

    @Override
    @Bean
    public int node() {
        return id.incrementAndGet();
    }
}