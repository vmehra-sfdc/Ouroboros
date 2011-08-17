package com.salesforce.ouroboros.spindle;

public interface Producer {
    Producer NULL_PRODUCER = new Producer() {
                               /* (non-Javadoc)
                                * @see com.salesforce.ouroboros.spindle.Producer#commit(com.salesforce.ouroboros.spindle.EventChannel, long, com.salesforce.ouroboros.spindle.EventHeader)
                                */
                               @Override
                               public void commit(EventChannel channel,
                                                  Segment segment, long offset,
                                                  EventHeader header) {
                                   // nuthin'
                               }
                           };

    void commit(EventChannel channel, Segment segment, long offset,
                EventHeader header);
}
