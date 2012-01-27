/**
 */
package com.salesforce.ouroboros.partition.functional.util;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode2Cfg;

/**
 * @author hhildebrand
 * 
 */
@Configuration
@Import({ nodeCfg.class })
public class wkMember2 extends GossipDiscoveryNode2Cfg {
    @Override
    protected int node() {
        return 1;
    }

}
