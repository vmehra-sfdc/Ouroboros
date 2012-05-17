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
package com.salesforce.ouroboros.spindle.replication;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.NullNode;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;
import com.salesforce.ouroboros.spindle.source.AbstractAppender;
import com.salesforce.ouroboros.spindle.source.Acknowledger;

/**
 * The appender for receiving duplicated events from the primary.
 * 
 * @author hhildebrand
 * 
 */
public class ReplicatingAppender extends AbstractAppender {

    private static final Logger log = LoggerFactory.getLogger(ReplicatingAppender.class.getCanonicalName());

    public ReplicatingAppender(final Bundle bundle) {
        super(bundle);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.AbstractAppender#commit()
     */
    @Override
    protected void commit() {
        try {
            eventChannel.append(batchHeader, offset, segment);
        } catch (IOException e) {
            log.error(String.format("Unable to append %s to segment on %s",
                                    batchHeader, segment, bundle.getId()));
            close();
            return;
        }
        Node node = batchHeader.getProducerMirror();
        Acknowledger acknowledger = bundle.getAcknowledger(node);
        if (acknowledger == null) {
            if (node.processId != NullNode.INSTANCE.processId) {
                log.warn(String.format("Could not find an acknowledger for %s",
                                       node));
            }
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Acknowledging replication of %s on %s",
                                    batchHeader, bundle.getId()));
        }
        acknowledger.acknowledge(batchHeader.getChannel(),
                                 batchHeader.getSequenceNumber());
    }

    @Override
    protected BatchHeader createBatchHeader() {
        return new ReplicatedBatchHeader();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.source.AbstractAppender#duplicatedBatch()
     */
    @Override
    protected void drain() {
        Node node = batchHeader.getProducerMirror();
        Acknowledger acknowledger = bundle.getAcknowledger(node);
        if (acknowledger == null) {
            if (node.processId != NullNode.INSTANCE.processId) {
                log.warn(String.format("Could not find an acknowledger for %s",
                                       node));
            }
            return;
        }
        if (log.isInfoEnabled()) {
            log.info(String.format("Acknowledging replication of duplicate %s:%s on %s",
                                   batchHeader.getChannel(),
                                   batchHeader.getSequenceNumber(),
                                   bundle.getId()));
        }
        acknowledger.acknowledge(batchHeader.getChannel(),
                                 batchHeader.getSequenceNumber());
        super.drain();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected AppendSegment getLogicalSegment() throws IOException {
        ReplicatedBatchHeader replicated = (ReplicatedBatchHeader) batchHeader;
        return eventChannel.appendSegmentFor(replicated.getOffset(),
                                             replicated.getPosition());
    }
}