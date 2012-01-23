package com.salesforce.ouroboros.spindle.functional.util;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.smartfrog.services.anubis.locator.AnubisLocator;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.Bean;

import com.fasterxml.uuid.Generators;
import com.hellblazer.jackal.gossip.configuration.GossipConfiguration;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.spindle.Coordinator;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.WeaverConfigation;

public class nodeCfg extends GossipConfiguration {
    public static final int MAX_SEGMENT_SIZE = 1024 * 1024;

    public static int       testPort1;
    public static int       testPort2;

    static {
        String port = System.getProperty("com.hellblazer.jackal.gossip.test.port.1",
                                         "24506");
        testPort1 = Integer.parseInt(port);
        port = System.getProperty("com.hellblazer.jackal.gossip.test.port.2",
                                  "24320");
        testPort2 = Integer.parseInt(port);
    }

    public final File       directory;

    public nodeCfg() {
        try {
            directory = File.createTempFile("CoordinatorIntegration-", ".root");
        } catch (IOException e) {
            throw new IllegalStateException();
        }
        directory.delete();
        directory.mkdirs();
        directory.deleteOnExit();
    }

    @Bean
    public Coordinator coordinator() throws IOException {
        return new Coordinator(timer(), switchboard(), weaver());
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
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partition(),
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Bean
    public ScheduledExecutorService timer() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    public Weaver weaver() throws IOException {
        return new Weaver(weaverConfiguration());
    }

    private WeaverConfigation weaverConfiguration() throws IOException {
        WeaverConfigation weaverConfigation = new WeaverConfigation();
        weaverConfigation.setId(memberNode());
        weaverConfigation.addRoot(directory);
        weaverConfigation.setMaxSegmentSize(maxSegmentSize());
        return weaverConfigation;
    }

    protected long maxSegmentSize() {
        return MAX_SEGMENT_SIZE;
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