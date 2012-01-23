package com.salesforce.ouroboros.spindle.functional.util;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.Configuration;

import com.hellblazer.jackal.gossip.configuration.ControllerGossipConfiguration;

@Configuration
public class MyControllerConfig extends ControllerGossipConfiguration {

    @Override
    public int magic() {
        try {
            return Identity.getMagicFromLocalIpAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Controller constructController() throws IOException {
        return new MyController(timer(), 1000, 300000, partitionIdentity(),
                                heartbeatTimeout(), heartbeatInterval(),
                                socketOptions(), dispatchExecutor(),
                                wireSecurity());
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