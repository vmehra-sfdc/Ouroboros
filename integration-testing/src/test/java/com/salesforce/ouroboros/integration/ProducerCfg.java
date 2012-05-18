/**
 */
package com.salesforce.ouroboros.integration;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerConfiguration;
import com.salesforce.ouroboros.producer.ProducerCoordinator;

@Configuration
public class ProducerCfg {
    private static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
                                                      int count = 0;

                                                      @Override
                                                      public Thread newThread(Runnable r) {
                                                          Thread daemon = new Thread(
                                                                                     r,
                                                                                     String.format("Producer worker[%s]",
                                                                                                   count++));
                                                          daemon.setDaemon(true);
                                                          return daemon;
                                                      }
                                                  });

    @Bean
    public ProducerConfiguration configuration() {
        return new ProducerConfiguration();
    }

    @Bean
    @Autowired
    public ProducerCoordinator coordinator(Switchboard switchboard,
                                           Producer producer)
                                                             throws IOException {
        return new ProducerCoordinator(switchboard, producer);
    }

    @Bean
    @Autowired
    public Node memberNode(Identity partitionIdentity) {
        return new Node(partitionIdentity.id);
    }

    @Bean
    @Autowired
    public Producer producer(Node memberNode) throws IOException {
        Source source = source();
        Producer producer = new Producer(memberNode, source, configuration());
        source.setProducer(producer);
        return producer;
    }

    @Bean
    public Source source() {
        return new Source(executor);
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
}