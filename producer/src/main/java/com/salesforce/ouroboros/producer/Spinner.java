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
package com.salesforce.ouroboros.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {

    public enum State {
        ERROR, ESTABLISHED, INITIAL;
    }

    public static final int                          HANDSHAKE_BYTE_SIZE = Node.BYTE_LENGTH + 4;
    public static final int                          MAGIC               = 0x1638;
    private final static Logger                      log                 = Logger.getLogger(Spinner.class.getCanonicalName());

    private final BatchAcknowledgement               ack;
    private final Coordinator                        coordinator;
    private SocketChannelHandler                     handler;
    private ByteBuffer                               handshake           = ByteBuffer.allocate(HANDSHAKE_BYTE_SIZE);
    private final NavigableMap<BatchIdentity, Batch> pending;
    private final AtomicReference<State>             state               = new AtomicReference<State>(
                                                                                                      State.INITIAL);
    private final BatchWriter                        writer;

    public Spinner(Coordinator coordinator) {
        writer = new BatchWriter();
        ack = new BatchAcknowledgement(this);
        pending = new ConcurrentSkipListMap<BatchIdentity, Batch>();
        this.coordinator = coordinator;
        handshake.putInt(MAGIC);
        coordinator.getId().serialize(handshake);
        handshake.flip();
    }

    /**
     * Acknowledge a successful commit of an event batch.
     * 
     * @param ack
     *            - the identity of the successfully commited event batch
     */
    public void acknowledge(BatchIdentity ack) {
        Batch batch = pending.remove(ack);
        if (batch != null) {
            coordinator.acknowledge(batch);
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Acknowledgement for %s on primary %s",
                                        ack, coordinator.getId()));
            }
        } else {
            coordinator.acknowledge(ack);
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Acknowledgement for %s on mirror %s",
                                        ack, coordinator.getId()));
            }
        }
    }

    /**
     * Close the receiver
     */
    public void close() {
        handler.close();
    }

    /**
     * Close the multiplexed channel handled by this spinner
     * 
     * @param channel
     *            - the id of the channel to close
     */
    public void close(UUID channel) {
        getPending(channel).clear();
    }

    @Override
    public void closing() {
        ack.closing();
        writer.closing();
    }

    public void failover() {
        writer.failover();
        ack.failover();
        handler.close();
    }

    /**
     * Retrieve all the unacknowledged batch events for the channel.
     * 
     * @param channel
     *            - the Channel
     * @return a SortedMap of all the batch events for the channel that have not
     *         been acknowledged. All pending batches for this channel are
     *         stored in ascending timestamp order
     */
    public SortedMap<BatchIdentity, Batch> getPending(UUID channel) {
        return pending.subMap(new BatchIdentity(channel, 0),
                              new BatchIdentity(channel, Long.MAX_VALUE));
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        ack.connect(handler);
        writer.connect(handler);
        writeHandshake();
    }

    @Override
    public void readReady() {
        ack.readReady();
    }

    @Override
    public void writeReady() {
        State s = state.get();
        switch (s) {
            case INITIAL:
                writeHandshake();
                break;
            case ESTABLISHED:
                writer.writeReady();
                break;
            default:
                log.severe(String.format("Illegal state for write: %s", s));
                handler.close();
        }
    }

    private void writeHandshake() {
        try {
            handler.getChannel().write(handshake);
        } catch (IOException e) {
            log.log(Level.WARNING, "Unable to write handshake", e);
            state.set(State.ERROR);
            handler.close();
            handshake = null;
            return;
        }
        if (!handshake.hasRemaining()) {
            state.set(State.ESTABLISHED);
            handshake = null;
        } else {
            handler.selectForWrite();
        }
    }

    /**
     * Push the batch of events.
     * 
     * @param events
     *            - the batch to push
     * @return true, if the push is allowed, false if the system is closed or in
     *         the error state.
     */
    public boolean push(Batch events) {
        return writer.push(events, pending);
    }

    public State getState() {
        return state.get();
    }
}
