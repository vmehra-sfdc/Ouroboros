package com.salesforce.ouroboros.partition.functional.util;

import static java.util.Arrays.asList;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.springframework.context.annotation.Configuration;

import com.salesforce.ouroboros.testUtils.PartitionControllerConfig;

@Configuration
public class MyControllerConfig extends PartitionControllerConfig {

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
