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
import java.util.concurrent.BrokenBarrierException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.XeroxContext.XeroxState;
import com.salesforce.ouroboros.util.Rendezvous;

/**
 * This class duplicates a channel state from the primary to the secondary.
 * 
 * @author hhildebrand
 * 
 */
public class Xerox implements CommunicationsHandler {

    public static final int      BUFFER_SIZE       = 8 + 8 + 8;
    public static final int      MAGIC             = 0x1638;
    private static final int     DEFAULT_TXFR_SIZE = 16 * 1024;
    private static final Logger  log               = Logger.getLogger(Xerox.class.getCanonicalName());

    private final ByteBuffer     buffer            = ByteBuffer.allocate(BUFFER_SIZE);
    private final UUID           channelId;
    private Segment              current;
    private final XeroxContext   fsm               = new XeroxContext(this);
    private SocketChannelHandler handler;
    private boolean              inError           = false;
    private final Node           node;
    private long                 position;
    private Rendezvous           rendezvous;
    private final Deque<Segment> segments;
    private long                 segmentSize;
    private final long           transferSize;

    public Xerox(Node toNode, UUID channel, Deque<Segment> segments) {
        this(toNode, channel, segments, DEFAULT_TXFR_SIZE);
    }

    public Xerox(Node toNode, UUID channel, Deque<Segment> segments,
                 int transferSize) {
        node = toNode;
        channelId = channel;
        this.transferSize = transferSize;
        this.segments = segments;
        buffer.putInt(MAGIC);
        buffer.putInt(segments.size());
        buffer.putLong(channelId.getMostSignificantBits());
        buffer.putLong(channelId.getLeastSignificantBits());
        buffer.flip();
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        for (Segment segment : segments) {
            try {
                segment.close();
            } catch (IOException e1) {
                log.log(Level.FINE,
                        String.format("Error closing: %s", segment), e1);
            }
        }
    }

    @Override
    public void connect(SocketChannelHandler h) {
        handler = h;
        fsm.connect();
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
    public void readReady() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param rendezvous
     *            the redezvous to set
     */
    public void setRendezvous(Rendezvous rendezvous) {
        this.rendezvous = rendezvous;
    }

    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    protected boolean copy() {
        long written;
        try {
            written = current.transferTo(position, transferSize,
                                         handler.getChannel());
        } catch (IOException e) {
            inError = true;
            log.log(Level.WARNING,
                    String.format("Error transfering %s on %s", current,
                                  handler.getChannel()), e);
            inError = true;
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

    protected void copySegment() {
        if (copy()) {
            fsm.copied();
        } else {
            handler.selectForWrite();
        }
    }

    protected void handshake() {
        if (writeHandshake()) {
            fsm.handshakeWritten();
        } else {
            handler.selectForWrite();
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextHeader() {
        position = 0;
        if (segments.isEmpty()) {
            try {
                rendezvous.meet();
            } catch (BrokenBarrierException e) {
                log.log(Level.SEVERE,
                        String.format("Rendezvous has already been met in xeroxing channel %s to %s",
                                      channelId, node), e);
            }
            fsm.finished();
            return;
        }
        current = segments.pop();
        try {
            segmentSize = current.size();
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error retrieving size of %s", current), e);
            inError = true;
            return;
        }
        buffer.clear();
        buffer.putLong(MAGIC);
        buffer.putLong(current.getPrefix());
        buffer.putLong(segmentSize);
        buffer.flip();

        if (writeHeader()) {
            fsm.initiateCopy();
        } else {
            handler.selectForWrite();
        }
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeHandshake() {
        try {
            handler.getChannel().write(buffer);
        } catch (IOException e) {
            log.log(Level.WARNING,
                    String.format("Error writing handshake for %s on %s",
                                  channelId, handler.getChannel()), e);
            inError = true;
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
            inError = true;
            return false;
        }
        return !buffer.hasRemaining();
    }
}
