package com.salesforce.ouroboros.producer.functional;

import java.io.IOException;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerConfiguration;
import com.salesforce.ouroboros.producer.ProducerCoordinator;

@Configuration
public class nodeCfg {
    @Autowired
    private Partition partitionManager;
    @Autowired
    private Identity  partitionIdentity;

    @Bean
    public ProducerCoordinator coordinator() throws IOException {
        return new ProducerCoordinator(switchboard(), producer());
    }

    @Bean
    public Node memberNode() {
        return new Node(partitionIdentity.id, partitionIdentity.id,
                        partitionIdentity.id);
    }

    @Bean
    public Producer producer() throws IOException {
        return new Producer(memberNode(), source(), configuration());
    }

    @Bean
    public EventSource source() {
        return new Source();
    }

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    protected ProducerConfiguration configuration() {
        return new ProducerConfiguration();
    }

}