/**
 */
package com.salesforce.ouroboros.partition.functional;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hellblazer.jackal.testUtil.gossip.GossipDiscoveryNode1Cfg;

/**
 * @author hhildebrand
 * 
 */
@Configuration
@Import({ nodeCfg.class })
public class wkMember1 extends GossipDiscoveryNode1Cfg {

    @Override
    protected int node() {
        return member.id.incrementAndGet();
    }

}
