package com.salesforce.ouroboros.partition;

import java.io.Serializable;


public class RingMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int          from;
    public Message            wrapped;

    public RingMessage(int from, Message wrapped) {
        this.from = from;
        this.wrapped = wrapped;
    }
}
