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
 * @author hhildebrand
 * 
 */
public class Weft implements BufferSerializable {
    public static int BYTE_SIZE = 8 + 8 + 8 + 4 + 4 + 4;

    public final UUID channel;
    public final int  endpoint;
    public final long eventId;
    public final int  packetSize;
    public final int  position;

    public Weft(ByteBuffer buffer) {
        channel = new UUID(buffer.getLong(), buffer.getLong());
        eventId = buffer.getLong();
        packetSize = buffer.getInt();
        position = buffer.getInt();
        endpoint = buffer.getInt();
    }

    public Weft(UUID channel, long eventId, int packetSize, int position,
                int endpoint) {
        this.channel = channel;
        this.eventId = eventId;
        this.position = position;
        this.endpoint = endpoint;
        this.packetSize = packetSize;
    }

    public void serializeOn(ByteBuffer buffer) {
        buffer.putLong(channel.getMostSignificantBits());
        buffer.putLong(channel.getLeastSignificantBits());
        buffer.putLong(eventId);
        buffer.putInt(packetSize);
        buffer.putInt(position);
        buffer.putInt(endpoint);
    }
}
