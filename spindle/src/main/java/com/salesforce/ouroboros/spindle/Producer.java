package com.salesforce.ouroboros.spindle;

public interface Producer {
    void commit(EventHeader header, long offset);

    Producer NULL_PRODUCER = new Producer() {
                               @Override
                               public void commit(EventHeader header,
                                                  long offset) {
                                   // nothin!
                               }
                           };
}
