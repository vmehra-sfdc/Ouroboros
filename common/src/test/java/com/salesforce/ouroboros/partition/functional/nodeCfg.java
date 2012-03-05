package com.salesforce.ouroboros.partition.functional;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;

@Configuration
public class nodeCfg {
    @Autowired
    private Identity  partitionIdentity;
    @Autowired
    private Partition partitionManager;

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        new M(memberNode(), switchboard);
        return switchboard;
    }

    protected Node memberNode() {
        return new Node(partitionIdentity.id, partitionIdentity.id,
                        partitionIdentity.id);
    }
}