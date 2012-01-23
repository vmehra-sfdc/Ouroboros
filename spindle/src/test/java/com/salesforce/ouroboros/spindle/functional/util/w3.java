package com.salesforce.ouroboros.spindle.functional.util;

import org.springframework.context.annotation.Configuration;

@Configuration public class w3 extends nodeCfg {
    @Override
    public int node() {
        return 3;
    }
}