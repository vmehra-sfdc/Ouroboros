package com.salesforce.ouroboros.spindle.functional.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;

@Configuration
@Import({ nodeCfg.class })
public class spindle extends GossipNodeCfg {
    private static final AtomicInteger id = new AtomicInteger(1);

    public static void reset() {
        id.set(1);
    }

    @Override
    @Bean
    public int node() {
        return id.incrementAndGet();
    }
}