/**
 */
package com.salesforce.ouroboros;

import java.util.List;

import com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy;

/**
 * @author hhildebrand
 * 
 */
public class NoSkipStrategy<T> implements SkipStrategy<T> {

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.ConsistentHashFunction.SkipStrategy#isSkippable(java.util.List, java.lang.Object)
     */
    @Override
    public boolean isSkippable(List<T> previous, T bucket) {
        return false;
    }

}
