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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.shuttle.ShuttleContext.ShuttleState;
import com.salesforce.ouroboros.util.Utils;

/**
 * @author hhildebrand
 * 
 */
public class Shuttle implements CommunicationsHandler {
    private static final Logger              log           = LoggerFactory.getLogger(Shuttle.class);

    private Node                             consumer;
    private Flyer                            currentFlyer;
    private boolean                          error         = false;
    private final ShuttleContext             fsm           = new ShuttleContext(
                                                                                this);
    private SocketChannelHandler             handler;
    private final ByteBuffer                 handshake     = ByteBuffer.allocate(Node.BYTE_LENGTH);
    private final WeftHeader                 header        = new WeftHeader();
    private int                              packetSize;
    private final Node                       self;
    private final ConcurrentMap<UUID, Flyer> subscriptions = new ConcurrentHashMap<>();

    public Shuttle(Node self) {
        this.self = self;
        fsm.setName(String.format("%s<?", self));
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        fsm.accept();
    }

    public void addSubscription(UUID client, Flyer flyer) {
        subscriptions.put(client, flyer);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        if (log.isInfoEnabled()) {
            log.info(String.format("Closing shuttle from %s to %s", self,
                                   consumer));
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

    public void removeSubscription(UUID client) {
        subscriptions.remove(client);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        fsm.writeReady();
    }

    /**
     * Initialize the current flyer from the weft header
     * 
     * @return true if the flyer was successfully acquired, false otherwise
     */
    private boolean setCurrentFlyer() {
        currentFlyer = subscriptions.get(header.getClientId());
        if (currentFlyer == null) {
            log.info(String.format("Span requested for client %s from consumer %s is not subscribed on %s",
                                   header.getClientId(), consumer, self));
            return false;
        }
        packetSize = header.getPacketSize();
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
    protected void nextWeft() {
        header.getBytes().rewind();
        while (readWeft()) {
            if (setCurrentFlyer()) {
                if (!writeSpan()) {
                    if (inError()) {
                        fsm.close();
                    } else {
                        selectForWrite();
                    }
                }
            }
            header.getBytes().rewind();
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
                                           self), e);
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
            fsm.setName(String.format("%s>%s", self, consumer));
            return true;
        }
    }

    /**
     * @return true if the weft header was completely read, false otherwise
     */
    protected boolean readWeft() {
        try {
            if (handler.getChannel().read(header.getBytes()) < 0) {
                error = true;
                return false;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception reading weft from %s on %s",
                                           consumer, self), e);
                }
            }
            error = true;
            return false;
        }
        return !header.getBytes().hasRemaining();
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
        PushResponse response;
        try {
            response = currentFlyer.push(handler.getChannel(), packetSize);
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("exception pushing span %s from %s to %s",
                                           currentFlyer.getCurrentSpan(), self,
                                           consumer), e);
                }
            }
            error = true;
            return false;
        }
        switch (response.writeStatus) {
            case CONTINUE:
                packetSize -= response.bytesWritten;
                return false;
            case SPAN_COMPLETE:
            case PACKET_COMPLETE:
            case NO_SPAN:
                return true;
            case SOCKET_CLOSED:
                error = true;
                return false;
            default:
                throw new IllegalStateException(
                                                String.format("Unknown write status: %s",
                                                              response.writeStatus));
        }
    }
}
