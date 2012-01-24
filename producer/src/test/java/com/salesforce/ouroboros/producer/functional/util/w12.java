package com.salesforce.ouroboros.producer.functional.util;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.springframework.context.annotation.Configuration;

@Configuration public class w12 extends nodeCfg {
    @Override
    public int node() {
        return 12;
    }

    @Override
    protected InetSocketAddress gossipEndpoint()
                                                throws UnknownHostException {
        return seedContact2();
    }
}