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
package com.salesforce.ouroboros;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;

/**
 * The header for a batch of events.
 * <p>
 * The header is comprised of:
 * 
 * <pre>
 *       4 byte byte length of the batch
 *       4 byte magic
 *      16 byte channel
 *       4 byte last event offset
 *       8 byte last event timestamp
 * </pre>
 * 
 * @author hhildebrand
 * 
 */
public class BatchHeader {
    protected static final int BATCH_BYTE_LENGTH_OFFSET = 0;
    protected static final int MAGIC_OFFSET             = BATCH_BYTE_LENGTH_OFFSET + 4;
    protected static final int CH_MSB_OFFSET            = MAGIC_OFFSET + 4;
    protected static final int CH_LSB_OFFSET            = CH_MSB_OFFSET + 8;
    protected static final int TIMESTAMP_OFFSET         = CH_LSB_OFFSET + 8;
    public static final int    HEADER_SIZE              = TIMESTAMP_OFFSET + 8;

    private final ByteBuffer bytes;

    public BatchHeader() {
        bytes = ByteBuffer.allocate(getHeaderSize());
    }

    public BatchHeader(ByteBuffer b) {
        bytes = b;
    }

    public BatchHeader(int batchByteLength, int magic, UUID channel,
                       long timestamp) {
        this();
        bytes.putInt(BATCH_BYTE_LENGTH_OFFSET, batchByteLength);
        bytes.putInt(MAGIC_OFFSET, magic);
        bytes.putLong(CH_MSB_OFFSET, channel.getMostSignificantBits());
        bytes.putLong(CH_LSB_OFFSET, channel.getLeastSignificantBits());
        bytes.putLong(TIMESTAMP_OFFSET, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BatchHeader) {
            BatchHeader b = (BatchHeader) o;
            return b.getMagic() == getMagic()
                   && b.getBatchByteLength() == getBatchByteLength()
                   && b.getTimestamp() == b.getTimestamp()
                   && b.getChannel().equals(getChannel());
        }
        return false;
    }

    /**
     * @return the batch byte length of the header
     */
    public int getBatchByteLength() {
        return bytes.getInt(BATCH_BYTE_LENGTH_OFFSET);
    }

    /**
     * @return the channel identifier for the event
     */
    public UUID getChannel() {
        return new UUID(bytes.getLong(CH_MSB_OFFSET),
                        bytes.getLong(CH_LSB_OFFSET));
    }

    /**
     * @return the magic value of the header
     */
    public int getMagic() {
        return bytes.getInt(MAGIC_OFFSET);
    }

    public long getTimestamp() {
        return bytes.getLong(TIMESTAMP_OFFSET);
    }

    @Override
    public int hashCode() {
        return getBatchByteLength();
    }

    /**
     * Read the header from the channel
     * 
     * @param channel
     *            - the channel to read from
     * @return true if the header has been completely read from the channel
     * @throws IOException
     */
    public boolean read(ReadableByteChannel channel) throws IOException {
        channel.read(bytes);
        return !bytes.hasRemaining();
    }

    /**
     * Rewind the byte content of the receiver
     */
    public void rewind() {
        bytes.rewind();
    }

    @Override
    public String toString() {
        return String.format("BatchHeader[magic=%s, timestamp=%s, length=%s, channel=%s]",
                             getMagic(), getTimestamp(), getBatchByteLength(),
                             getChannel());
    }

    /**
     * Write the byte contents of the receiver on the channel
     * 
     * @param channel
     *            - the channel to write the contents of the receiver
     * @return true if all the bytes of the receiver have been written to the
     *         channel, false if bytes are still remaining
     * @throws IOException
     *             - if problems occur during write
     */
    public boolean write(WritableByteChannel channel) throws IOException {
        channel.write(bytes);
        return !bytes.hasRemaining();
    }

    protected int getHeaderSize() {
        return HEADER_SIZE;
    }

    public ByteBuffer getBytes() {
        return bytes;
    }
}
