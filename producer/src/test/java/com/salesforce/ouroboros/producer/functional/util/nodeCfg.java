package com.salesforce.ouroboros.producer.functional.util;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.Bean;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.gossip.configuration.GossipConfiguration;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.EventSource;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.producer.Coordinator;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.producer.ProducerConfiguration;

public class nodeCfg extends GossipConfiguration {
    public static int testPort1;
    public static int testPort2;
    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "25230");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "25370");
        testPort2 = Integer.parseInt(port);
    }

    @Bean
    public Coordinator coordinator() throws IOException {
        return new Coordinator(switchboard(), producer());
    }

    @Override
    public int getMagic() {
        try {
            return Identity.getMagicFromLocalIpAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @Bean
    public AnubisLocator locator() {
        return null;
    }

    @Bean
    public Node memberNode() {
        return new Node(node(), node(), node());
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
                                                  partition(),
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    protected ProducerConfiguration configuration() {
        return new ProducerConfiguration();
    }

    @Override
    protected Collection<InetSocketAddress> seedHosts()
                                                       throws UnknownHostException {
        return asList(seedContact1(), seedContact2());
    }

    InetSocketAddress seedContact1() throws UnknownHostException {
        return new InetSocketAddress("127.0.0.1", testPort1);
    }

    InetSocketAddress seedContact2() throws UnknownHostException {
        return new InetSocketAddress("127.0.0.1", testPort2);
    }
}