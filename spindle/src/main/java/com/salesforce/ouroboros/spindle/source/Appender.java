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
package com.salesforce.ouroboros.spindle.source;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.NullNode;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;
import com.salesforce.ouroboros.spindle.replication.EventEntry;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Appender extends AbstractAppender {
    private final static Logger log          = LoggerFactory.getLogger(Appender.class.getCanonicalName());

    private final Acknowledger  acknowledger;
    private volatile int        startPosition;
    private EventEntry          freeList;
    private final ReentrantLock freeListLock = new ReentrantLock();

    public Appender(Bundle bundle, Acknowledger acknowledger) {
        super(bundle);
        this.acknowledger = acknowledger;
    }

    /**
     * @param fsmName
     */
    public void setFsmName(String fsmName) {
        fsm.setName(fsmName);
    }

    @Override
    protected void commit() {
        try {
            EventEntry entry = allocate();
            entry.set(batchHeader, offset, startPosition, eventChannel,
                      eventChannel.getCachedReadSegment(segment.getFile()),
                      acknowledger, handler);
            Node producerMirror = batchHeader.getProducerMirror();
            Acknowledger mirrorAcknowledger = null;
            if (producerMirror.processId != NullNode.INSTANCE.processId) {
                mirrorAcknowledger = bundle.getAcknowledger(producerMirror);
                if (mirrorAcknowledger == null) {
                    log.warn(String.format("mirror acknowledger to %s for %s is missing on %s",
                                           producerMirror, batchHeader,
                                           bundle.getId()));
                }
            }
            eventChannel.append(entry, mirrorAcknowledger);
        } catch (IOException e) {
            log.error(String.format("Unable to append to %s for %s at %s on %s",
                                    segment, batchHeader, offset,
                                    bundle.getId()));
            close();
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Committed %s on %s ", batchHeader,
                                    bundle.getId()));
        }
    }

    @Override
    protected BatchHeader createBatchHeader() {
        return new BatchHeader();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.source.AbstractAppender#drain()
     */
    @Override
    protected void drain() {
        acknowledger.acknowledge(batchHeader.getChannel(),
                                 batchHeader.getSequenceNumber());
        if (log.isTraceEnabled()) {
            log.trace(String.format("Acknowledging replication of duplicate %s:%s on %s",
                                    batchHeader.getChannel(),
                                    batchHeader.getSequenceNumber(),
                                    bundle.getId()));
        }
        Acknowledger mirrorAcknowledger = bundle.getAcknowledger(batchHeader.getProducerMirror());
        if (mirrorAcknowledger == null) {
            if (batchHeader.getProducerMirror().processId != NullNode.INSTANCE.processId) {
                log.warn(String.format("Could not find an acknowledger for %s",
                                       batchHeader.getProducerMirror()));
            }
        } else {
            mirrorAcknowledger.acknowledge(batchHeader.getChannel(),
                                           batchHeader.getSequenceNumber());
            if (log.isTraceEnabled()) {
                log.trace(String.format("Acknowledging mirror replication of duplicate %s:%s on %s",
                                        batchHeader.getChannel(),
                                        batchHeader.getSequenceNumber(),
                                        bundle.getId()));
            }
        }
        super.drain();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected AppendSegment getLogicalSegment() throws IOException {
        return eventChannel.appendSegmentFor(batchHeader);
    }

    @Override
    protected void markPosition() {
        startPosition = position;
    }

    public void free(EventEntry free) {
        final ReentrantLock lock = freeListLock;
        lock.lock();
        try {
            freeList = free.link(freeList);
        } finally {
            lock.unlock();
        }
    }

    private EventEntry allocate() {
        final ReentrantLock lock = freeListLock;
        lock.lock();
        try {
            if (freeList == null) {
                return new EventEntry(this);
            }
            EventEntry allocated = freeList;
            freeList = allocated.delink();
            return allocated;
        } finally {
            lock.unlock();
        }
    }
}
