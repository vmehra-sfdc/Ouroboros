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

/**
 * The header containing the metadata of an Event.
 * 
 * An event header comprised of:
 * 
 * <pre>
 *       4 byte size
 *       4 byte magic
 *       4 byte CRC32
 * </pre>
 * 
 * @author hhildebrand
 * 
 */
public class EventHeader implements Cloneable {

    protected static final int SIZE_OFFSET      = 0;
    protected static final int MAGIC_OFFSET     = SIZE_OFFSET + 4;
    protected static final int CRC_OFFSET       = MAGIC_OFFSET + 8;
    public static final int    HEADER_BYTE_SIZE = CRC_OFFSET + 4;

    protected final ByteBuffer bytes;

    public EventHeader(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public EventHeader(int size, int magic, int crc32) {
        this(ByteBuffer.allocate(HEADER_BYTE_SIZE));
        initialize(size, magic, crc32);
    }

    /**
     * Clear the bytes
     */
    public void clear() {
        bytes.clear();
    }

    @Override
    public EventHeader clone() {
        ByteBuffer duplicateBytes = ByteBuffer.allocate(HEADER_BYTE_SIZE);
        bytes.rewind();
        duplicateBytes.put(bytes);
        return new EventHeader(duplicateBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EventHeader)) {
            return false;
        }
        EventHeader header = (EventHeader) o;
        return header.getMagic() == getMagic() && header.size() == size()
               && header.getCrc32() == getCrc32();
    }

    /**
     * @return the CRC32 value of the payload
     */
    public int getCrc32() {
        return bytes.getInt(CRC_OFFSET);
    }

    /**
     * @return the magic value of the header
     */
    public int getMagic() {
        return bytes.getInt(MAGIC_OFFSET);
    }

    @Override
    public int hashCode() {
        return getCrc32();
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

    /**
     * @return the size of the payload
     */
    public int size() {
        return bytes.getInt(SIZE_OFFSET);
    }

    @Override
    public String toString() {
        return String.format("EventHeader[size=%s, magic=%s, crc=%s", size(),
                             getMagic(), getCrc32());
    }

    public int totalSize() {
        return HEADER_BYTE_SIZE + size();
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

    protected void initialize(int size, int magic, int crc32) {
        bytes.putInt(SIZE_OFFSET, size);
        bytes.putInt(MAGIC_OFFSET, magic);
        bytes.putInt(CRC_OFFSET, crc32);
    }
}