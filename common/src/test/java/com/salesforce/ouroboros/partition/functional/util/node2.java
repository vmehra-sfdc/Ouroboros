package com.salesforce.ouroboros.partition.functional.util;

import org.springframework.context.annotation.Configuration;


@Configuration public class node2 extends nodeCfg {
    @Override
    public int node() {
        return 2;
    }
}