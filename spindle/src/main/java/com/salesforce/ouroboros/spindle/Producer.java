package com.salesforce.ouroboros.spindle;

public interface Producer {
    void commit(EventChannel channel, Segment segment, long offset,
                EventHeader header);
}
