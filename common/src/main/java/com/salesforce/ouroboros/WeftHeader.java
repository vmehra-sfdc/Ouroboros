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

/**
 * @author hhildebrand
 * 
 */
public class WeftHeader {
    private static int       CHANNEL_ID_MSB_INDEX = 0;
    private static int       CHANNEL_ID_LSB_INDEX = CHANNEL_ID_MSB_INDEX + 8;
    private static int       EVENT_ID_INDEX       = CHANNEL_ID_LSB_INDEX + 8;
    private static int       PACKET_SIZE_INDEX    = EVENT_ID_INDEX + 8;
    private static int       POSITION_INDEX       = PACKET_SIZE_INDEX + 4;
    private static int       ENDPOINT_INDEX       = POSITION_INDEX + 4;
    public static int        HEADER_BYTE_SIZE     = ENDPOINT_INDEX + 4;

    private final ByteBuffer bytes;

    public WeftHeader() {
        this(ByteBuffer.allocateDirect(HEADER_BYTE_SIZE));
    }

    public WeftHeader(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    /**
     * @return the bytes
     */
    public ByteBuffer getBytes() {
        return bytes;
    }

    public UUID getChannel() {
        return new UUID(bytes.getLong(CHANNEL_ID_MSB_INDEX),
                        bytes.getLong(CHANNEL_ID_LSB_INDEX));
    }

    public int getPacketSize() {
        return bytes.getInt(PACKET_SIZE_INDEX);
    }

    public int getPosition() {
        return bytes.getInt(POSITION_INDEX);
    }

    public int getEndpoint() {
        return bytes.getInt(ENDPOINT_INDEX);
    }

    public long getEventId() {
        return bytes.getLong(EVENT_ID_INDEX);
    }

    public void set(UUID channel, long eventId, int packetSize, int position,
                    int endpoint) {
        bytes.putLong(CHANNEL_ID_MSB_INDEX, channel.getMostSignificantBits());
        bytes.putLong(CHANNEL_ID_LSB_INDEX, channel.getLeastSignificantBits());
        bytes.putLong(EVENT_ID_INDEX, eventId);
        bytes.putInt(PACKET_SIZE_INDEX, packetSize);
        bytes.putInt(POSITION_INDEX, position);
        bytes.putInt(ENDPOINT_INDEX, endpoint);
    }
}
