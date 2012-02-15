/**
 */
package com.salesforce.ouroboros;

import java.util.List;

import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * @author hhildebrand
 *
 */

/**
 * Default skip strategy which skips nodes that are marked as down, or share the
 * same rack, machine or releaseGroup id
 * 
 */
public class DefaultSkipStrategy implements
        ConsistentHashFunction.SkipStrategy<Node> {
    @Override
    public boolean isSkippable(List<Node> previous, Node bucket) {
        if (bucket.isDown()) {
            return true;
        }
        for (Node node : previous) {
            if (node.machineId == bucket.machineId
                || node.rackId == bucket.rackId
                || node.releaseGroup == bucket.releaseGroup) {
                return true;
            }
        }
        return false;
    }
}