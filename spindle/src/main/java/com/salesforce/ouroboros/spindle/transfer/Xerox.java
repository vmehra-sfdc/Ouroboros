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
package com.salesforce.ouroboros.spindle.transfer;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.transfer.XeroxContext.XeroxState;
import com.salesforce.ouroboros.util.Rendezvous;
import com.salesforce.ouroboros.util.Utils;

/**
 * This class duplicates a channel state from the primary to the secondary.
 * 
 * @author hhildebrand
 * 
 */
public class Xerox implements CommunicationsHandler {

    public static final int           BUFFER_SIZE = 8 + 8 + 8;
    public static final int           MAGIC       = 0x1638;
    private static final Logger       log         = LoggerFactory.getLogger(Xerox.class.getCanonicalName());

    private final ByteBuffer          buffer      = ByteBuffer.allocate(BUFFER_SIZE);
    private final Deque<EventChannel> channels    = new LinkedList<EventChannel>();
    private EventChannel              currentChannel;
    private Segment                   currentSegment;
    private final XeroxContext        fsm         = new XeroxContext(this);
    private SocketChannelHandler      handler;
    private boolean                   inError     = false;
    private final Node                to;
    private final Node                from;
    private long                      position;
    private Rendezvous                rendezvous;
    private Deque<Segment>            segments;
    private long                      segmentSize;
    private long                      transferSize;

    public Xerox(Node from, Node toNode) {
        fsm.setName(String.format("%s->%s", from.processId, toNode.processId));
        this.from = from;
        to = toNode;
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public void addChannel(EventChannel channel) {
        channels.add(channel);
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
    }

    @Override
    public void connect(SocketChannelHandler h) {
        handler = h;
        try {
            transferSize = handler.getChannel().socket().getSendBufferSize();
        } catch (SocketException e) {
            transferSize = 16 * 1024;
            log.warn(String.format("Cannot retrieve send buffer size from: %s in %s",
                                   handler.getChannel().socket(), fsm.getName()));
        }
        fsm.connect();
    }

    /**
     * @return the node
     */
    public Node getNode() {
        return to;
    }

    public XeroxState getState() {
        return fsm.getState();
    }

    @Override
    public void readReady() {
        fsm.readReady();
    }

    /**
     * @param rendezvous
     *            the rendezvous to set
     */
    public void setRendezvous(Rendezvous rendezvous) {
        this.rendezvous = rendezvous;
    }

    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    protected void cancelRendezvous() {
        rendezvous.cancel();
    }

    protected boolean copy() {
        long written;
        try {
            written = currentSegment.transferTo(position, transferSize,
                                                handler.getChannel());
        } catch (IOException e) {
            log.warn(String.format("Error transfering %s on %s",
                                   currentSegment, idString()), e);
            inError = true;
            return false;
        }
        position += written;
        if (log.isTraceEnabled()) {
            log.trace(String.format("Copying %s, written=%s, position=%s on %s to %s",
                                    currentSegment, written, position, from, to));
        }
        if (position == segmentSize) {
            return true;
        }
        return false;
    }

    protected void copySegment() {
        buffer.clear();
        if (copy()) {
            fsm.finished();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected void nextChannel() {
        if (channels.isEmpty()) {
            fsm.channelsEmpty();
            return;
        }
        currentChannel = channels.pop();
        segments = currentChannel.getSegmentStack();
        if (log.isInfoEnabled()) {
            log.info(String.format("Starting Xerox of %s from %s to %s, segments: %s",
                                   currentChannel.getId(), from, to,
                                   segments.size()));
        }
        buffer.clear();
        buffer.limit(Sink.CHANNEL_HEADER_SIZE);
        buffer.putInt(MAGIC);
        buffer.putInt(segments.size());
        buffer.putLong(currentChannel.getId().getMostSignificantBits());
        buffer.putLong(currentChannel.getId().getLeastSignificantBits());
        buffer.flip();

        if (writeChannelHeader()) {
            fsm.finished();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
        }
    }

    protected void nextSegment() {
        if (segments.isEmpty()) {
            fsm.finished();
            return;
        }
        position = 0;
        currentSegment = segments.pop();
        try {
            segmentSize = currentSegment.size();
        } catch (IOException e) {
            log.warn(String.format("Error retrieving size of %s on %s",
                                   currentSegment, idString()), e);
            inError = true;
            return;
        }
        if (log.isInfoEnabled()) {
            log.info(String.format("Starting Xerox of segment %s, total size= %s, from %s to %s",
                                   currentSegment, segmentSize, from, to));
        }
        buffer.clear();
        buffer.limit(Sink.SEGMENT_HEADER_SIZE);
        buffer.putInt(MAGIC);
        buffer.putLong(currentSegment.getPrefix());
        buffer.putLong(segmentSize);
        buffer.flip();

        if (writeSegmentHeader()) {
            fsm.initiateCopy();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
        }
    }

    protected boolean readAck() {
        try {
            if (handler.getChannel().read(buffer) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Closing channel on %s", idString()));
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            log.warn(String.format("Error reading acknowledgement on %s",
                                   idString()), e);
            inError = true;
            return false;
        }
        if (!buffer.hasRemaining()) {
            buffer.flip();
            if (buffer.getInt() != MAGIC) {
                log.error(String.format("Invalid acknowlegement from %s on %s",
                                        to, from));
                inError = true;
                return false;
            }
            if (log.isTraceEnabled()) {
                log.trace(String.format("Ack received from %s on %s", to, from));
            }
            try {
                rendezvous.meet();
            } catch (BrokenBarrierException e) {
                log.error(String.format("Rendezvous has been cancelled in xeroxing %s to %s",
                                        from, to));
            }
            return true;
        }
        return false;
    }

    protected void receiveAck() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Complete, waiting for ack on %s from %s",
                                    from, to));
        }
        buffer.clear();
        buffer.limit(Sink.ACK_HEADER_SIZE);
        if (readAck()) {
            fsm.finished();
        } else {
            handler.selectForRead();
        }
    }

    protected void selectForRead() {
        handler.selectForRead();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected void sendHandshake() {
        buffer.clear();
        buffer.limit(Sink.HANDSHAKE_HEADER_SIZE);
        buffer.putInt(MAGIC);
        buffer.putInt(from.processId);
        buffer.putInt(channels.size());
        buffer.flip();
        if (writeHandshake()) {
            fsm.finished();
        } else {
            if (inError) {
                fsm.close();
            } else {
                handler.selectForWrite();
            }
        }
    }

    protected boolean writeHandshake() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Closing channel from %s to %s",
                                            from, to));
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing xerox %s ", fsm.getName()));
            } else {
                log.warn(String.format("Error writing handshake for %s to %s on %s",
                                       currentSegment, idString(), idString()),
                         e);
            }
            inError = true;
            return false;
        }
        return !buffer.hasRemaining();
    }

    protected boolean writeChannelHeader() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Closing channel from %s to %s",
                                            from, to));
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            log.warn(String.format("Error writing channel header %s for %s on %s",
                                   currentChannel, currentChannel.getId(),
                                   idString()), e);
            inError = true;
            return false;
        }
        if (!buffer.hasRemaining()) {
            return true;
        }
        return false;
    }

    protected boolean writeSegmentHeader() {
        try {
            if (handler.getChannel().write(buffer) < 0) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Closing channel from %s to %s",
                                            from, to));
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            if (Utils.isClose(e)) {
                log.info(String.format("closing xerox %s ", fsm.getName()));
            } else {
                log.warn(String.format("Error writing header for %s on %s",
                                       currentSegment, idString()), e);
            }
            inError = true;
            return false;
        }
        return !buffer.hasRemaining();
    }

    String idString() {
        return String.format("%s to %s", from, to);
    }
}
