package com.salesforce.ouroboros.spindle.functional.util;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.springframework.context.annotation.Configuration;

@Configuration public class w1 extends nodeCfg {
    @Override
    public int node() {
        return 1;
    }

    @Override
    protected InetSocketAddress gossipEndpoint()
                                                throws UnknownHostException {
        return seedContact1();
    }
}