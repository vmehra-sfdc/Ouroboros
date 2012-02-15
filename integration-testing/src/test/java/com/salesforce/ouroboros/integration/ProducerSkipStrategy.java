/**
 */
package com.salesforce.ouroboros.integration;

import java.util.List;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.producer.Producer;
import com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy;

/**
 * @author hhildebrand
 * 
 */
public class ProducerSkipStrategy implements SkipStrategy<Producer> {
    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy#isSkippable(java.util.List, java.lang.Object)
     */
    @Override
    public boolean isSkippable(List<Producer> previous, Producer bucket) {
        if (bucket.getId().isDown()) {
            return true;
        }
        for (Producer p : previous) {
            Node node = p.getId();
            if (node.machineId == bucket.getId().machineId
                || node.rackId == bucket.getId().rackId
                || node.releaseGroup == bucket.getId().releaseGroup) {
                return true;
            }
        }
        return false;
    }

}
