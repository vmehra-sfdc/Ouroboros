package com.salesforce.ouroboros.producer.functional;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import com.salesforce.ouroboros.api.producer.EventSource;

public class Source implements EventSource {

    @Override
    public void assumePrimary(Map<UUID, Long> newPrimaries) {
    }

    @Override
    public void closed(UUID channel) {
    }

    @Override
    public void deactivated(Collection<UUID> deadChannels) {
    }

    @Override
    public void opened(UUID channel) {
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#pauseChannels(java.util.Collection)
     */
    @Override
    public void pause(Collection<UUID> pausedChannels) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#relinquishPrimary(java.util.Collection)
     */
    @Override
    public void relinquishPrimary(Collection<UUID> channels) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.api.producer.EventSource#resume(java.util.Collection)
     */
    @Override
    public void resume(Collection<UUID> pausedChannels) {
        // TODO Auto-generated method stub

    }

}