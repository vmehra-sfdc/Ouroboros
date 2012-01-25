package com.salesforce.ouroboros.testUtils;

import java.io.IOException;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.context.annotation.Configuration;

import com.hellblazer.jackal.gossip.configuration.ControllerGossipConfiguration;

@Configuration
public class PartitionControllerConfig extends ControllerGossipConfiguration {

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
        return new PartitionController(partitionIdentity(), heartbeatTimeout(),
                                       heartbeatInterval(), socketOptions(),
                                       dispatchExecutor(), wireSecurity());
    }
}