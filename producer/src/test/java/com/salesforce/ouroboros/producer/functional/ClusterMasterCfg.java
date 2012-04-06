package com.salesforce.ouroboros.producer.functional;

import java.io.IOException;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.testUtil.TestNodeCfg;
import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;

@Configuration
public class ClusterMasterCfg extends GossipNodeCfg {
    @Autowired
    private Partition partitionManager;
    private int       node = -1;

    @Bean
    public ClusterMaster clusterMaster() {
        return new ClusterMaster(switchboard());
    }

    @Override
    public int getMagic() {
        return TestNodeCfg.getMagicValue();
    }

    @Bean
    public Node memberNode() {
        return new Node(node(), node(), node());
    }

    @Override
    public int node() {
        if (node == -1) {
            node = weaver.id.incrementAndGet();
        }
        return node;
    }

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }
}