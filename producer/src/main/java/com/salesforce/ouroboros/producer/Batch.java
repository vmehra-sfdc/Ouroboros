package com.salesforce.ouroboros.producer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

public class Batch {
    public final UUID                   channel;
    public final Collection<ByteBuffer> events;
    public final long                   timestamp;

    public Batch(UUID channel, long timestamp, Collection<ByteBuffer> events) {
        this.channel = channel;
        this.timestamp = timestamp;
        this.events = events;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Batch other = (Batch) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        } else if (!channel.equals(other.channel))
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

}
