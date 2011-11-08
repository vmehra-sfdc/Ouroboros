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
package com.salesforce.ouroboros.spindle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.XeroxContext.XeroxState;

/**
 * This class duplicates a channel state from the primary to the secondary.
 * 
 * @author hhildebrand
 * 
 */
public class Xerox implements CommunicationsHandler {

    public static final long     MAGIC             = 0x1638L;
    private static final int     BUFFER_SIZE       = 8 + 8 + 8;
    private static final int     DEFAULT_TXFR_SIZE = 16 * 1024;
    private static final Logger  log               = Logger.getLogger(Xerox.class.getCanonicalName());

    private final ByteBuffer     buffer            = ByteBuffer.allocate(BUFFER_SIZE);
    private final UUID           channelId;
    private Segment              current;
    private SocketChannelHandler handler;
    private CountDownLatch       latch;
    private final Node           node;
    private long                 position;
    private final Deque<Segment> segments;
    private long                 segmentSize;
    private final long           transferSize;
    private boolean              inError           = false;
    private final XeroxContext   fsm               = new XeroxContext(this);

    public Xerox(Node toNode, UUID channel, Deque<Segment> segments) {
        this(toNode, channel, segments, DEFAULT_TXFR_SIZE);
    }

    public Xerox(Node toNode, UUID channel, Deque<Segment> segments,
                 int transferSize) {
        node = toNode;
        channelId = channel;
        this.transferSize = transferSize;
        this.segments = segments;
        buffer.putLong(MAGIC);
        buffer.putLong(channelId.getMostSignificantBits());
        buffer.putLong(channelId.getLeastSignificantBits());
        buffer.flip();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
    }

    /**
     * @return the node
     */
    public Node getNode() {
        return node;
    }

    public XeroxState getState() {
        return fsm.getState();
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    protected void finished() {
        // TODO
    }

    @Override
    public void connect(SocketChannelHandler h) {
        handler = h;
        fsm.connect();
    }

    @Override
    public void readReady() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    /**
     * @param latch
     *            the latch to set
     */
    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    protected void writeCopy() {
        while (copy())
            ;
    }

    protected boolean copy() {
        long written;
        try {
            written = current.transferTo(position, transferSize,
                                         handler.getChannel());
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error transfering %s on %s", current,
                                  handler.getChannel()), e);
            terminate();
            return false;
        }
        position += written;
        if (position == segmentSize) {
            try {
                current.close();
            } catch (IOException e) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", current), e);
            }
            return true;
        }
        return false;
    }

    protected boolean segmentCopied() {
        return position == segmentSize;
    }

    protected boolean writeHandshake() {
        try {
            handler.getChannel().write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing handshake for %s on %s",
                                  channelId, handler.getChannel()), e);
            terminate();
            return false;
        }
        if (!buffer.hasRemaining()) {
            return true;
        }
        return false;
    }

    protected boolean writeHeader() {
        try {
            handler.getChannel().write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing header for %s on %s", current,
                                  handler.getChannel()), e);
            terminate();
            return false;
        }
        return !buffer.hasRemaining();
    }

    protected void sendNextSegment() {
        position = 0;
        if (segments.isEmpty()) {
            latch.countDown();
            fsm.finished();
            return;
        }
        current = segments.pop();
        try {
            segmentSize = current.size();
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error retrieving size of %s", current), e);
            terminate();
            return;
        }
        buffer.clear();
        buffer.putLong(MAGIC);
        buffer.putLong(current.getPrefix());
        buffer.putLong(segmentSize);
        buffer.flip();
        if (writeHeader()) {
            fsm.initiateCopy();
        }
    }

    private void terminate() {
        inError = true;
        for (Segment segment : segments) {
            try {
                segment.close();
            } catch (IOException e1) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", segment), e1);
            }
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected boolean bufferIsWritten() {
        return !buffer.hasRemaining();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }
}
