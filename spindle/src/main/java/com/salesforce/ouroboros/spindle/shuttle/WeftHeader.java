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
package com.salesforce.ouroboros.spindle.shuttle;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * @author hhildebrand
 * 
 */
public class WeftHeader {
    private static int       CLIENT_ID_MSB_INDEX = 0;
    private static int       CLIENT_ID_LSB_INDEX = CLIENT_ID_MSB_INDEX + 8;
    private static int       HEADER_BYTE_SIZE    = CLIENT_ID_LSB_INDEX + 8;
    private static byte[]    END_MARKER;

    static {
        END_MARKER = new byte[HEADER_BYTE_SIZE];
        Arrays.fill(END_MARKER, (byte) 0);
    }

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

    public UUID getClientId() {
        return new UUID(bytes.getLong(CLIENT_ID_MSB_INDEX),
                        bytes.getLong(CLIENT_ID_LSB_INDEX));
    }

    public boolean isEndMarker() {
        return Arrays.equals(END_MARKER, bytes.array());
    }

    public void setClientId(UUID clientId) {
        bytes.putLong(CLIENT_ID_MSB_INDEX, clientId.getMostSignificantBits());
        bytes.putLong(CLIENT_ID_LSB_INDEX, clientId.getLeastSignificantBits());
    }

    public void setEndMarker() {
        bytes.rewind();
        bytes.put(END_MARKER);
    }
}
