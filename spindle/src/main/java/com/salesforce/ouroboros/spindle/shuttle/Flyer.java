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
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import com.salesforce.ouroboros.spindle.EventChannel;

/**
 * 
 * “For Fate has wove the thread of life with pain And twins even from the birth
 * are Misery and Man”
 * 
 * @author hhildebrand
 * 
 */
public class Flyer {

    private volatile EventSpan      current;
    private final SpanHeader        header        = new SpanHeader();
    private volatile long           position;
    private final Set<EventChannel> subscriptions = new HashSet<>();
    private final Deque<EventSpan>  thread        = new LinkedBlockingDeque<>();

    /**
     * Add the event span to the flyer's thread of events
     * 
     * @param span
     */
    public void deliver(EventSpan span) {
        thread.add(span);
    }

    /**
     * Push events from the flyer's thread to the socket channel. Deliver only
     * the maximum number of bytes indicated.
     * 
     * @param channel
     *            - the channel to write events to
     * @param maxBytes
     *            - the maximum number of bytes to write to the channel
     * @return - the number of bytes written
     * @throws IOException
     *             if something goes wrong with the transfer
     */
    public long push(SocketChannel channel, long maxBytes) throws IOException {
        long written = 0;
        long remainingBytes = maxBytes;
        do {
            long currentWrite = pushSpan(channel, remainingBytes);
            if (currentWrite < 0) {
                written = currentWrite;
                break;
            } else if (currentWrite == 0) {
                break;
            }
            written += currentWrite;
            remainingBytes -= currentWrite;
        } while (remainingBytes > 0);
        return written;
    }

    /**
     * Subscribe to the event channel, listening for events after the last event
     * offset
     * 
     * @param channel
     *            - the channel we're listening to
     * @param lastEventOffset
     *            - the offset of the last event seen on this channel, or -1 to
     *            retreive only events after subscribing
     * @throws IOException
     *             if something goes wrong when subscribing
     */
    public void subscribe(EventChannel channel, long lastEventOffset)
                                                                     throws IOException {
        subscriptions.add(channel);
        channel.subscribe(this, lastEventOffset);
    }

    /**
     * Load the next span in the thread
     * 
     * @return true if a new event span is available for push, false if there
     *         are no spans left
     */
    private boolean loadNextSpan() {
        current = thread.poll();
        if (current != null) {
            position = current.offset;
            header.set(SpanHeader.MAGIC,
                       current.segment.getEventChannel().getId(),
                       current.eventId,
                       (int) (current.endpoint - current.offset));
            header.clear();
            return true;
        } else {
            position = -1L;
            return false;
        }
    }

    /**
     * Push a span of events from the thread to the socket channel. Deliver only
     * the maximum number of bytes indicated.
     * 
     * @param channel
     *            - the channel to push the events
     * @param maxBytes
     *            - the maximum number of event bytes to send
     * @return the number of bytes actually delivered
     * @throws IOException
     *             if something goes wrong with the transfer
     */
    private long pushSpan(SocketChannel channel, long maxBytes)
                                                               throws IOException {
        long written = 0;
        if (current == null) {
            if (loadNextSpan()) {
                written = channel.write(header.getBytes());
                if (written < 0 || header.getBytes().hasRemaining()) {
                    return written;
                }
                maxBytes -= written;
                long currentWrite = writeCurrentSpan(channel, maxBytes);
                if (currentWrite < 0) {
                    return currentWrite;
                }
                written += currentWrite;
            }
        } else {
            if (header.getBytes().hasRemaining()) {
                written = channel.write(header.getBytes());
                maxBytes -= written;
            }
            if (written < 0 || header.getBytes().hasRemaining()) {
                return written;
            }
            long currentWrite = writeCurrentSpan(channel, maxBytes);
            if (currentWrite < 0) {
                return currentWrite;
            }
            written += currentWrite;
        }
        return written;
    }

    /**
     * Write the current event span out, up to a maximum number of bytes
     * 
     * @param channel
     *            - the socket channel to write to
     * @param maxBytes
     *            - the maximum number of bytes to be written
     * @return the number of bytes actually written
     * @throws IOException
     *             - if anything goes wrong with the write
     */
    private long writeCurrentSpan(SocketChannel channel, long maxBytes)
                                                                       throws IOException {
        maxBytes = Math.min(maxBytes, current.endpoint - position);
        long written = current.segment.transferTo(position, maxBytes, channel);
        if (written > 0) {
            position += written;
            if (position == current.endpoint) {
                current = null;
                position = -1L;
            }
        }
        return written;
    }

}