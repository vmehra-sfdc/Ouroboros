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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.spindle.Bundle;
import com.salesforce.ouroboros.spindle.EventChannel.AppendSegment;
import com.salesforce.ouroboros.spindle.replication.ReplicatedBatchHeader;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Appender extends AbstractAppender {
    private final static Logger log = Logger.getLogger(Appender.class.getCanonicalName());

    private final Acknowledger  acknowledger;
    private volatile int        startPosition;

    public Appender(Bundle bundle, Acknowledger acknowledger) {
        super(bundle);
        this.acknowledger = acknowledger;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected void commit() {
        try {
            ReplicatedBatchHeader batchHeader2 = new ReplicatedBatchHeader(
                                                                           batchHeader,
                                                                           offset,
                                                                           startPosition);
            eventChannel.append(batchHeader2, segment, acknowledger, handler);
        } catch (IOException e) {
            if (log.isLoggable(Level.SEVERE)) {
                log.log(Level.SEVERE,
                        String.format("Unable to append to %s for %s at %s on %s",
                                      segment, batchHeader, offset,
                                      bundle.getId()));
            }
            close();
        }
        if (log.isLoggable(Level.FINER)) {
            log.fine(String.format("Committed %s on %s ", batchHeader,
                                   bundle.getId()));
        }
    }

    @Override
    protected BatchHeader createBatchHeader() {
        return new BatchHeader();
    }

    @Override
    protected AppendSegment getLogicalSegment() {
        return eventChannel.segmentFor(batchHeader);
    }

    @Override
    protected void markPosition() {
        startPosition = (int) position;
    }

    /**
     * @param fsmName
     */
    public void setFsmName(String fsmName) {
        fsm.setName(fsmName);
    }
}
