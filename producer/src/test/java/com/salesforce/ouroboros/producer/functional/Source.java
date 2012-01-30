package com.salesforce.ouroboros.producer.functional;

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
    public void opened(UUID channel) {
    }

}