/**
 */
package com.salesforce.ouroboros.integration;

import java.io.IOException;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.producer.ProducerCoordinator;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerConfiguration;

@Configuration
public class ProducerCfg {

    @Bean
    public ProducerConfiguration configuration() {
        return new ProducerConfiguration();
    }

    @Bean
    @Autowired
    public ProducerCoordinator coordinator(Switchboard switchboard, Producer producer)
                                                                              throws IOException {
        return new ProducerCoordinator(switchboard, producer);
    }

    @Bean
    @Autowired
    public Switchboard switchboard(Node memberNode, Partition partitionManager) {
        Switchboard switchboard = new Switchboard(
                                                  memberNode,
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Bean
    @Autowired
    public Node memberNode(Identity partitionIdentity) {
        return new Node(partitionIdentity.id);
    }

    @Bean
    @Autowired
    public Producer producer(Node memberNode) throws IOException {
        return new Producer(memberNode, source(), configuration());
    }

    @Bean
    public Source source() {
        return new Source();
    }
}