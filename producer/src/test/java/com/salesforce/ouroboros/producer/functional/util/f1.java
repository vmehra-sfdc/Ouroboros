package com.salesforce.ouroboros.producer.functional.util;

import org.springframework.context.annotation.Configuration;


@Configuration public class f1 extends FakeSpindleCfg {
    @Override
    public int node() {
        return 1;
    }
}