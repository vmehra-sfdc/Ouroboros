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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.Weft;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.spindle.shuttle.ShuttleContext.ShuttleState;
import com.salesforce.ouroboros.util.Utils;

/**
 * @author hhildebrand
 * 
 */
public class Shuttle implements CommunicationsHandler {
    private static final Logger  log        = LoggerFactory.getLogger(Shuttle.class);
    private static final int     MAX_AGENDA = 1000;

    private Node                 consumer;
    private boolean              error      = false;
    private final ShuttleContext fsm        = new ShuttleContext(this);
    private SocketChannelHandler handler;
    private final ByteBuffer     handshake  = ByteBuffer.allocate(Node.BYTE_LENGTH);
    private long                 eventId;
    private UUID                 channel;
    private int                  endpoint;
    private int                  position;
    private int                  packetSize;
    private Segment              segment;
    private final Bundle         bundle;
    private final ByteBuffer     agenda     = ByteBuffer.allocateDirect(MAX_AGENDA
                                                                        * Weft.BYTE_SIZE);

    public Shuttle(Bundle bundle) {
        this.bundle = bundle;
        fsm.setName(String.format("%s<?", bundle.getId()));
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.accept();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        if (log.isInfoEnabled()) {
            log.info(String.format("Closing shuttle from %s to %s",
                                   bundle.getId(), consumer));
        }
        fsm.close();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#connect(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void connect(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    public ShuttleState getState() {
        return fsm.getState();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#readReady()
     */
    @Override
    public void readReady() {
        fsm.readReady();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    /**
     * Initialize the current flyer
     * 
     * @return true if the flyer was successfully acquired, false otherwise
     */
    private boolean setCurrentFlyer() {
        Weft weft = new Weft(agenda);
        channel = weft.channel;
        eventId = weft.eventId;
        packetSize = weft.packetSize;
        position = weft.position;
        endpoint = weft.endpoint;
        EventChannel eventChannel = bundle.eventChannelFor(channel);
        if (eventChannel == null) {
            log.warn(String.format("Span requested for channel %s from consumer %s does not exist on %s",
                                   channel, consumer, bundle.getId()));
            return false;
        }

        try {
            segment = eventChannel.segmentFor(eventId).segment;
        } catch (IOException e) {
            log.warn(String.format("Segment requested for event %s on channel %s from consumer %s does not exist on %s",
                                   eventId, channel, consumer, bundle.getId()),
                     e);
            return false;
        }
        return true;
    }

    protected void close() {
        handler.close();
    }

    protected void handshake() {
        if (!readHandshake()) {
            if (inError()) {
                fsm.close();
            } else {
                selectForRead();
            }
        } else {
            fsm.established();
        }
    }

    /**
     * @return true if the receiver has experienced an error, false otherwise
     */
    protected boolean inError() {
        return error;
    }

    /**
     * Write the next span to the consumer
     */
    protected void nextSpan() {
        if (!setCurrentFlyer()) {
            fsm.unknownSubscription();
            return;
        }

        if (!writeSpan()) {
            if (inError()) {
                fsm.close();
            } else {
                selectForWrite();
            }
        }
    }

    /**
     * batch all the things. Keep reading and writing until we can't read or
     * write any more
     */
    protected void advanceAgenda() {
        while (readAgenda()) {
            agenda.flip();
            while (agenda.remaining() >= Weft.BYTE_SIZE) {
                if (setCurrentFlyer()) {
                    if (!writeSpan()) {
                        if (inError()) {
                            fsm.close();
                        } else {
                            agenda.compact();
                            agenda.flip();
                            fsm.writeSpan();
                        }
                        return;
                    }
                }
            }
            agenda.compact();
        }

        if (inError()) {
            fsm.close();
        } else {
            selectForRead();
        }
    }

    /**
     * @return true if the handshake was completely read, false otherwise
     */
    protected boolean readHandshake() {
        try {
            if (handler.getChannel().read(handshake) < 0) {
                error = true;
                return false;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception reading handshake on %s",
                                           bundle.getId()), e);
                }
            }
            error = true;
            return false;
        }
        if (handshake.hasRemaining()) {
            return false;
        } else {
            handshake.flip();
            consumer = new Node(handshake);
            fsm.setName(String.format("%s>%s", bundle.getId(), consumer));
            return true;
        }
    }

    /**
     * @return true if the read agenda has at least one header available, false
     *         otherwise
     */
    protected boolean readAgenda() {
        try {
            if (handler.getChannel().read(agenda) < 0) {
                error = true;
                return false;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception reading weft from %s on %s",
                                           consumer, bundle.getId()), e);
                }
            }
            error = true;
            return false;
        }
        return agenda.position() >= Weft.BYTE_SIZE;
    }

    protected void selectForRead() {
        handler.selectForRead();
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    /**
     * @return true if the span packet was completely written, false otherwise
     */
    protected boolean writeSpan() {
        int maxBytes = Math.min(packetSize, endpoint - position);
        long written;
        try {
            written = segment.transferTo(position, maxBytes,
                                         handler.getChannel());
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception pushing span %s:%s from %s to %s",
                                           channel, eventId, bundle.getId(),
                                           consumer), e);
                }
            }
            error = true;
            return false;
        }
        if (written < 0) {
            inError();
            return false;
        }
        position += written;
        if (position == endpoint) {
            return true;
        }
        packetSize -= written;
        return false;
    }
}
