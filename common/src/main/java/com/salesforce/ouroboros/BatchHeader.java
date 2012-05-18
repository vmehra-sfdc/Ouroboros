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
import java.nio.MappedByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;

import com.salesforce.ouroboros.util.Utils;

/**
 * The header for a batch of events.
 * <p>
 * The header is comprised of:
 * 
 * <pre>
 *       4 byte byte length of the batch
 *       4 byte magic
 *       4 byte producer mirror node process id
 *      16 byte channel
 *       4 byte last event offset
 *       8 byte last event sequence number
 * </pre>
 * 
 * @author hhildebrand
 * 
 */
public class BatchHeader {
    public static final int    MAGIC                    = 0x1638;
    protected static final int BATCH_BYTE_LENGTH_OFFSET = 0;
    protected static final int MAGIC_OFFSET             = BATCH_BYTE_LENGTH_OFFSET + 4;
    protected static final int PRODUCER_MIRROR_OFFSET   = MAGIC_OFFSET + 4;
    protected static final int CH_MSB_OFFSET            = PRODUCER_MIRROR_OFFSET + 4;
    protected static final int CH_LSB_OFFSET            = CH_MSB_OFFSET + 8;
    protected static final int SEQUENCE_NUMBER_OFFSET   = CH_LSB_OFFSET + 8;
    public static final int    HEADER_BYTE_SIZE         = SEQUENCE_NUMBER_OFFSET + 8;

    /**
     * @param bytes
     * @param mirror
     * @param batchByteSize
     * @param magic
     * @param channel
     * @param sequenceNumber
     */
    public static void append(ByteBuffer bytes, Node mirror, int batchByteSize,
                              int magic, UUID channel, long sequenceNumber) {
        int position = bytes.position();
        bytes.putInt(position + BATCH_BYTE_LENGTH_OFFSET, batchByteSize);
        bytes.putInt(position + MAGIC_OFFSET, magic);
        bytes.putInt(position + PRODUCER_MIRROR_OFFSET, mirror.processId);
        bytes.putLong(position + CH_MSB_OFFSET,
                      channel.getMostSignificantBits());
        bytes.putLong(position + CH_LSB_OFFSET,
                      channel.getLeastSignificantBits());
        bytes.putLong(position + SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        bytes.position(position + HEADER_BYTE_SIZE);
    }

    public static BatchHeader readFrom(ByteBuffer buffer) {
        ByteBuffer slice = buffer.slice();
        buffer.position(buffer.position() + HEADER_BYTE_SIZE);
        return new BatchHeader(slice.asReadOnlyBuffer());
    }

    protected final ByteBuffer bytes;

    public BatchHeader() {
        bytes = ByteBuffer.allocateDirect(getHeaderSize());
    }

    public BatchHeader(ByteBuffer b) {
        bytes = b;
    }

    public BatchHeader(Node mirror, int batchByteLength, int magic,
                       UUID channel, long sequenceNumber) {
        this();
        set(mirror, batchByteLength, magic, channel, sequenceNumber);
    }

    public void clear() {
        bytes.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BatchHeader) {
            BatchHeader b = (BatchHeader) o;
            return b.getMagic() == getMagic()
                   && b.getProducerMirror().equals(getProducerMirror())
                   && b.getBatchByteLength() == getBatchByteLength()
                   && b.getSequenceNumber() == b.getSequenceNumber()
                   && b.getChannel().equals(getChannel());
        }
        return false;
    }

    public void free() {
        if (bytes instanceof MappedByteBuffer) {
            Utils.unmap((MappedByteBuffer) bytes);
        }
    }

    /**
     * @return the batch byte length of the header
     */
    public int getBatchByteLength() {
        return bytes.getInt(BATCH_BYTE_LENGTH_OFFSET);
    }

    public ByteBuffer getBytes() {
        return bytes;
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

    public Node getProducerMirror() {
        return new Node(bytes.getInt(PRODUCER_MIRROR_OFFSET));
    }

    public long getSequenceNumber() {
        return bytes.getLong(SEQUENCE_NUMBER_OFFSET);
    }

    @Override
    public int hashCode() {
        return getBatchByteLength();
    }

    public boolean hasRemaining() {
        return bytes.hasRemaining();
    }

    /**
     * Read the header from the channel
     * 
     * @param channel
     *            - the channel to read from
     * @return the number of bytes read, or -1 if the channel is closed.
     * @throws IOException
     */
    public int read(ReadableByteChannel channel) throws IOException {
        return channel.read(bytes);
    }

    public void resetMirror(Node mirror) {
        bytes.putInt(PRODUCER_MIRROR_OFFSET, mirror.processId);
    }

    /**
     * Rewind the byte content of the receiver
     */
    public void rewind() {
        bytes.rewind();
    }

    public void set(Node mirror, int batchByteLength, int magic, UUID channel,
                    long sequenceNumber) {
        bytes.rewind();
        append(bytes, mirror, batchByteLength, magic, channel, sequenceNumber);
    }

    @Override
    public String toString() {
        return String.format("BatchHeader[magic=%s, sequenceNumber=%s, length=%s, channel=%s]",
                             getMagic(), getSequenceNumber(),
                             getBatchByteLength(), getChannel());
    }

    /**
     * Write the byte contents of the receiver on the channel
     * 
     * @param channel
     *            - the channel to write the contents of the receiver
     * @return the number of bytes written, or -1 if the channel is closed
     * @throws IOException
     *             - if problems occur during write
     */
    public int write(WritableByteChannel channel) throws IOException {
        return channel.write(bytes);
    }

    protected int getHeaderSize() {
        return HEADER_BYTE_SIZE;
    }
}
