package com.salesforce.ouroboros.producer.functional;

import org.smartfrog.services.anubis.partition.Partition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.testUtil.gossip.GossipNodeCfg;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;

public class FakeSpindleCfg extends GossipNodeCfg {

    @Autowired
    private Partition partitionManager;
    private int       node = -1;

    @Bean
    public Node memberNode() {
        return new Node(node(), node(), node());
    }

    @Override
    @Bean
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
        new FakeSpindle(switchboard);
        return switchboard;
    }
}