/**
 */
package com.salesforce.ouroboros.integration;

import java.util.List;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy;

/**
 * @author hhildebrand
 * 
 */
public class WeaverSkipStrategy implements SkipStrategy<Weaver> {

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy#isSkippable(java.util.List, java.lang.Object)
     */
    @Override
    public boolean isSkippable(List<Weaver> previous, Weaver bucket) {
        if (bucket.getId().isDown()) {
            return true;
        }
        for (Weaver w : previous) {
            Node node = w.getId();
            if (node.machineId == bucket.getId().machineId
                || node.rackId == bucket.getId().rackId
                || node.releaseGroup == bucket.getId().releaseGroup) {
                return true;
            }
        }
        return false;
    }

}
