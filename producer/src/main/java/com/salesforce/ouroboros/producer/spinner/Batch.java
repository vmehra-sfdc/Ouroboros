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
package com.salesforce.ouroboros.producer.spinner;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

import com.salesforce.ouroboros.BatchIdentity;
import com.salesforce.ouroboros.Node;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Batch extends BatchIdentity {
    public final Collection<ByteBuffer> events;
    public final Node                   mirror;
    private final long                  created = System.currentTimeMillis();

    public Batch(Node mirror, UUID channel, long timestamp,
                 Collection<ByteBuffer> events) {
        super(channel, timestamp);
        assert events != null : "events must not be null";
        this.events = events;
        this.mirror = mirror;
    }

    /**
     * @return the interval, in milliseconds, between when the batch was
     *         submitted and when it was acknowledged
     */
    public int interval() {
        return (int) (System.currentTimeMillis() - created);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Batch [#events=" + events.size() + ", created=" + created
               + ", channel=" + channel + ", timestamp=" + timestamp + "]";
    }
}
