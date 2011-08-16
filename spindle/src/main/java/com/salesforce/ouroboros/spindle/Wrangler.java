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
package com.salesforce.ouroboros.spindle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Wrangler implements Bundle {
    public static File channelFor(File root, UUID channel) {
        return new File(root, channel.toString().replace('-', '/'));
    }

    private final File                        root;
    private final long                        maxSegmentSize;
    private ConcurrentMap<UUID, EventChannel> openChannels = new ConcurrentHashMap<UUID, EventChannel>();

    public Wrangler(final File root, final long maxSegmentSize) {
        this.root = root;
        this.maxSegmentSize = maxSegmentSize;
    }

    @Override
    public Segment appendSegmentFor(EventHeader header)
                                                       throws FileNotFoundException {
        UUID channelTag = header.getChannel();
        EventChannel channel = openChannels.get(channelTag);
        if (channel == null) {
            throw new IllegalStateException(
                                            String.format("No open channel: %s",
                                                          channelTag));
        }
        File segment = new File(
                                channelFor(channelTag),
                                channel.appendSegmentNameFor((int) header.totalSize(),
                                                             maxSegmentSize));
        FileOutputStream fos = new FileOutputStream(segment, true);
        return new Segment(fos);
    }

    public File channelFor(UUID channelTag) {
        return new File(root, channelTag.toString().replace('-', '/'));
    }

    @Override
    public Segment segmentFor(long offset, EventHeader header) {
        // TODO Auto-generated method stub
        return null;
    }
}
