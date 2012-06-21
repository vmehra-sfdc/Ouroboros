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

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.batch.BatchWriter;
import com.salesforce.ouroboros.spindle.EventChannel;
import com.salesforce.ouroboros.spindle.Segment;
import com.salesforce.ouroboros.util.Pool;
import com.salesforce.ouroboros.util.Pool.Clearable;

/**
 * The entry representing the appending of an event.
 * 
 * @author hhildebrand
 * 
 */
public class EventEntry implements Clearable {
    private final Pool<EventEntry>              pool;
    private volatile BatchWriter<BatchIdentity> acknowledger;
    private volatile EventChannel               eventChannel;
    private volatile SocketChannelHandler       handler;
    private final ReplicatedBatchHeader         header = new ReplicatedBatchHeader();
    private volatile Segment                    segment;

    public EventEntry(Pool<EventEntry> pool) {
        this.pool = pool;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.util.Pool.Clearable.clear()
     */
    @Override
    public void clear() {
        acknowledger = null;
        eventChannel = null;
        handler = null;
        segment = null;
        header.clear();
    }

    public void free() {
        if (pool != null) {
            pool.free(this);
        }
    }

    /**
     * @return the acknowledger
     */
    public BatchWriter<BatchIdentity> getAcknowledger() {
        return acknowledger;
    }

    /**
     * @return the eventChannel
     */
    public EventChannel getEventChannel() {
        return eventChannel;
    }

    /**
     * @return the header
     */
    public ReplicatedBatchHeader getHeader() {
        return header;
    }

    /**
     * @return the segment
     */
    public Segment getSegment() {
        return segment;
    }

    public void select() {
        handler.selectForRead();
    }

    public void selectAndFree() {
        select();
        free();
    }

    public void set(BatchHeader batchHeader, long offset, int startPosition,
                    EventChannel eventChannel, Segment segment,
                    BatchWriter<BatchIdentity> acknowledger,
                    SocketChannelHandler handler) {
        assert batchHeader != null : "Batch header cannot be null";
        assert eventChannel != null : "Event channel cannot be null";
        assert segment != null : "Segment cannot be null";
        assert acknowledger != null : "Acknowledger cannot be null";
        assert handler != null : "Handler cannot be null";

        header.set(batchHeader, offset, startPosition);
        this.eventChannel = eventChannel;
        this.segment = segment;
        this.acknowledger = acknowledger;
        this.handler = handler;
    }
}
