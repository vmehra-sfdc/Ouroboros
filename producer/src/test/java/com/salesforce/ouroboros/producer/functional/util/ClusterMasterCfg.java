package com.salesforce.ouroboros.producer.functional.util;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.gossip.configuration.GossipConfiguration;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;

@Configuration
public class ClusterMasterCfg extends GossipConfiguration {
    @Bean
    public ClusterMaster clusterMaster() {
        return new ClusterMaster(switchboard());
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

    @Override
    public int node() {
        return 0;
    }

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partition(),
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Override
    protected Collection<InetSocketAddress> seedHosts()
                                                       throws UnknownHostException {
        return asList(seedContact1(), seedContact2());
    }

    InetSocketAddress seedContact1() throws UnknownHostException {
        return new InetSocketAddress("127.0.0.1", nodeCfg.testPort1);
    }

    InetSocketAddress seedContact2() throws UnknownHostException {
        return new InetSocketAddress("127.0.0.1", nodeCfg.testPort2);
    }
}