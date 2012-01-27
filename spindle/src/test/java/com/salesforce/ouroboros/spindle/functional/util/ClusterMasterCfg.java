package com.salesforce.ouroboros.spindle.functional.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.smartfrog.services.anubis.partition.Partition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;

@Configuration
public class ClusterMasterCfg extends GossipNodeCfg {
    @Autowired
    private Partition partitionManager;

    @Bean
    public ClusterMaster clusterMaster() {
        return new ClusterMaster(switchboard());
    }

    @Bean
    public Node memberNode() {
        return new Node(node(), node(), node());
    }

    @Override
    public int node() {
        return 0;
    }

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Bean
    public ScheduledExecutorService timer() {
        return Executors.newSingleThreadScheduledExecutor();
    }
}