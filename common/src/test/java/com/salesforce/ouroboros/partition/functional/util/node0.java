package com.salesforce.ouroboros.partition.functional.util;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.springframework.context.annotation.Configuration;


@Configuration public class node0 extends nodeCfg {
    @Override
    public int node() {
        return 0;
    }

    @Override
    protected InetSocketAddress gossipEndpoint()
                                                throws UnknownHostException {
        return seedContact1();
    }
}