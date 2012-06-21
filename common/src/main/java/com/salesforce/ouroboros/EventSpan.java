/**
 * Copyright (c) 2012, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.ouroboros;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.salesforce.ouroboros.batch.BufferSerializable;

/**
 * Marks a span of events in a channel's segment
 * 
 * @author hhildebrand
 * 
 */
public class EventSpan implements BufferSerializable {
    public static final int BYTE_SIZE = 5 * 8;

    // the id of the first event of the span
    public final long       eventId;
    // The end point offset of the span within the segment
    public final long       endpoint;
    // The offset of the first event within the segment
    public final long       offset;
    // The id of the channel
    public final UUID       channel;

    public EventSpan(ByteBuffer buffer) {
        this.eventId = buffer.getLong();
        this.channel = new UUID(buffer.getLong(), buffer.getLong());
        this.offset = buffer.getLong();
        this.endpoint = buffer.getLong();
    }

    /**
     * @param segment
     * @param offset
     */
    public EventSpan(long eventId, UUID channel, long offset, int length) {
        this.eventId = eventId;
        this.channel = channel;
        this.offset = offset;
        this.endpoint = offset + length;
    }

    public void serializeOn(ByteBuffer buffer) {
        buffer.putLong(eventId);
        buffer.putLong(channel.getMostSignificantBits());
        buffer.putLong(channel.getLeastSignificantBits());
        buffer.putLong(offset);
        buffer.putLong(endpoint);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + (int) (endpoint ^ (endpoint >>> 32));
        result = prime * result + (int) (eventId ^ (eventId >>> 32));
        result = prime * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        EventSpan other = (EventSpan) obj;
        if (channel == null) {
            if (other.channel != null) {
                return false;
            }
        } else if (!channel.equals(other.channel)) {
            return false;
        }
        if (endpoint != other.endpoint) {
            return false;
        }
        if (eventId != other.eventId) {
            return false;
        }
        if (offset != other.offset) {
            return false;
        }
        return true;
    }
}