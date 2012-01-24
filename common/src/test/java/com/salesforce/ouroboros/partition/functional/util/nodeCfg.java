package com.salesforce.ouroboros.partition.functional.util;

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
import com.salesforce.ouroboros.partition.Switchboard;

public class nodeCfg extends GossipConfiguration {
    public static int testPort1;
    public static int testPort2;
    
    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "24110");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "24220");
        testPort2 = Integer.parseInt(port);
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
    public M member() {
        return new M(memberNode(), switchboard());
    }

    @Bean
    public Node memberNode() {
        return new Node(node(), node(), node());
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
        return new InetSocketAddress("127.0.0.1", testPort1);
    }

    InetSocketAddress seedContact2() throws UnknownHostException {
        return new InetSocketAddress("127.0.0.1", testPort2);
    }
}