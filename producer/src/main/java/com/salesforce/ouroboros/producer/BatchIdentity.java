/**
 * Copyright (c) 2011, salesforce.com, inc.
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
package com.salesforce.ouroboros.producer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 
 * @author hhildebrand
 * 
 */
public class BatchIdentity implements Comparable<BatchIdentity> {
    public static final int BYTE_SIZE = 3 * 8;

    public final UUID       channel;
    public final long       timestamp;

    public BatchIdentity(ByteBuffer buffer) {
        channel = new UUID(buffer.getLong(), buffer.getLong());
        timestamp = buffer.getLong();
    }

    public BatchIdentity(UUID channel, long timestamp) {
        this.channel = channel;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(BatchIdentity bi) {
        int channelComp = channel.compareTo(bi.channel);
        if (channelComp == 0) {
            return (int) (timestamp - bi.timestamp);
        }
        return channelComp;
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
        Batch other = (Batch) obj;
        if (channel == null) {
            if (other.channel != null) {
                return false;
            }
        } else if (!channel.equals(other.channel)) {
            return false;
        }
        if (timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (channel == null ? 0 : channel.hashCode());
        result = prime * result + (int) (timestamp ^ timestamp >>> 32);
        return result;
    }

    public void serializeOn(ByteBuffer buffer) {
        buffer.putLong(channel.getMostSignificantBits());
        buffer.putLong(channel.getLeastSignificantBits());
        buffer.putLong(timestamp);
    }
}
