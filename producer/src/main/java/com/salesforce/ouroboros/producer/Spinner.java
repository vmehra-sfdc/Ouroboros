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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.api.producer.RateLimiteExceededException;
import com.salesforce.ouroboros.producer.SpinnerContext.SpinnerFSM;
import com.salesforce.ouroboros.producer.SpinnerContext.SpinnerState;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {

    public static final int                          HANDSHAKE_BYTE_SIZE = Node.BYTE_LENGTH + 4;
    public static final int                          MAGIC               = 0x1638;
    private final static Logger                      log                 = Logger.getLogger(Spinner.class.getCanonicalName());

    private final BatchAcknowledgement               ack                 = new BatchAcknowledgement(
                                                                                                    this);
    private final Producer                           producer;
    private final SpinnerContext                     fsm                 = new SpinnerContext(
                                                                                              this);
    private SocketChannelHandler                     handler;
    private ByteBuffer                               handshake           = ByteBuffer.allocate(HANDSHAKE_BYTE_SIZE);
    private boolean                                  inError;
    private final NavigableMap<BatchIdentity, Batch> pending             = new ConcurrentSkipListMap<BatchIdentity, Batch>();
    private final BatchWriter                        writer;

    public Spinner(Producer producer, int maxQueueLength) {
        this.producer = producer;
        handshake.putInt(MAGIC);
        producer.getId().serialize(handshake);
        handshake.flip();
        writer = new BatchWriter(maxQueueLength);
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
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
            producer.acknowledge(batch);
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Acknowledgement for %s on primary %s",
                                        ack, producer.getId()));
            }
        } else {
            producer.acknowledge(ack);
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Acknowledgement for %s on mirror %s",
                                        ack, producer.getId()));
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

    @Override
    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        ack.connect(handler);
        writer.connect(handler);
        fsm.handshake();
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

    public SpinnerState getState() {
        return fsm.getState();
    }

    public boolean isEstablished() {
        return fsm.getState() == SpinnerFSM.Established;
    }

    /**
     * Push the batch of events.
     * 
     * @param events
     *            - the batch to push
     * @throws RateLimiteExceededException
     *             - if the batch throughput rate has been exceeded.
     */
    public void push(Batch events) throws RateLimiteExceededException {
        writer.push(events, pending);
    }

    @Override
    public void readReady() {
        ack.readReady();
    }

    @Override
    public void writeReady() {
        if (fsm.getState() == SpinnerFSM.Established) {
            writer.writeReady();
        } else {
            fsm.writeReady();
        }
    }

    protected boolean inError() {
        return inError;
    }

    protected void processHandshake() {
        if (writeHandshake()) {
            fsm.established();
        } else {
            handler.selectForWrite();
        }
    }

    protected void selectForWrite() {
        handler.selectForWrite();
    }

    protected boolean writeHandshake() {
        try {
            if (handler.getChannel().write(handshake) < 0) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Closing channel");
                }
                inError = true;
                return false;
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Unable to write handshake", e);
            inError = true;
            handshake = null;
            return false;
        }
        return !handshake.hasRemaining();
    }
}
