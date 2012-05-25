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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.spindle.EventChannel.Role;
import com.salesforce.ouroboros.util.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestEventChannel {

    private File root;

    @Before
    public void setUp() throws Exception {
        root = File.createTempFile("TestEventChannel", ".root");
        root.delete();
        root.mkdirs();
    }

    @After
    public void teardown() {
        if (root != null) {
            Utils.deleteDirectory(root);
        }
    }

    @Test
    public void testPrefix() {
        assertEquals(0L, EventChannel.prefixFor(666L, 1024L));
        assertEquals(1024L, EventChannel.prefixFor(1024, 1024L));
        assertEquals(1024L, EventChannel.prefixFor(1025, 1024L));
        assertEquals(2048, EventChannel.prefixFor(2048, 1024L));
        assertEquals(1024 * 11, EventChannel.prefixFor(1024 * 11, 1024L));
        assertEquals(1024 * 10, EventChannel.prefixFor(1024 * 11 - 1, 1024L));
        assertEquals(1024 * 11, EventChannel.prefixFor(1024 * 11 + 15, 1024L));
    }

    @Test
    public void testSegmentGeneration() throws Exception {
        long maxSegmentSize = 16 * 1024;
        int eventSize = 256;
        Node node = new Node(0);
        Node secondary = new Node(1);
        UUID channel = UUID.randomUUID();
        int sequenceNumber = 0;
        @SuppressWarnings("unchecked")
        ConcurrentMap<File, Segment> segmentCache = mock(ConcurrentMap.class);
        EventChannel eventChannel = new EventChannel(node, Role.PRIMARY,
                                                     secondary, channel, root,
                                                     maxSegmentSize, null,
                                                     segmentCache, segmentCache);
        long offset = 0;
        BatchHeader batchHeader;
        AppendSegment logicalSegment;

        // For the first segment, offset and position should track
        for (; offset + eventSize < maxSegmentSize; offset += eventSize) {
            batchHeader = new BatchHeader(node, eventSize, 666, channel,
                                          sequenceNumber++);
            logicalSegment = eventChannel.appendSegmentFor(batchHeader);
            assertNotNull(logicalSegment);
            assertEquals(offset, logicalSegment.offset);
            assertEquals(offset, logicalSegment.position);
            assertEquals(0L, logicalSegment.segment.getPrefix());
            eventChannel.append(batchHeader, offset, logicalSegment.segment);
        }

        // the next event should trigger a segment rollover
        batchHeader = new BatchHeader(node, eventSize, 666, channel,
                                      sequenceNumber++);
        logicalSegment = eventChannel.appendSegmentFor(batchHeader);
        assertNotNull(logicalSegment);
        assertTrue(offset < logicalSegment.offset);
        assertEquals(0, logicalSegment.position);
        assertEquals(maxSegmentSize, logicalSegment.segment.getPrefix());
        eventChannel.append(batchHeader, logicalSegment.offset,
                            logicalSegment.segment);

        long position = logicalSegment.position + eventSize;
        offset = logicalSegment.offset + eventSize;

        // Now track the second rollover segment.
        for (; offset + eventSize < 2 * maxSegmentSize; offset += eventSize) {
            batchHeader = new BatchHeader(node, eventSize, 666, channel,
                                          sequenceNumber++);
            logicalSegment = eventChannel.appendSegmentFor(batchHeader);
            assertNotNull(logicalSegment);
            assertEquals(offset, logicalSegment.offset);
            assertEquals(position, logicalSegment.position);
            assertEquals(maxSegmentSize, logicalSegment.segment.getPrefix());
            eventChannel.append(batchHeader, offset, logicalSegment.segment);
            position += eventSize;
        }

        // the next event should trigger another segment rollover
        batchHeader = new BatchHeader(node, eventSize, 666, channel,
                                      sequenceNumber++);
        logicalSegment = eventChannel.appendSegmentFor(batchHeader);
        assertNotNull(logicalSegment);
        assertTrue(offset < logicalSegment.offset);
        assertEquals(0, logicalSegment.position);
        assertEquals(maxSegmentSize * 2, logicalSegment.segment.getPrefix());
        eventChannel.append(batchHeader, logicalSegment.offset,
                            logicalSegment.segment);

        position = logicalSegment.position + eventSize;
        offset = logicalSegment.offset + eventSize;

        // Now track the second rollover segment.
        for (; offset + eventSize < 3 * maxSegmentSize; offset += eventSize) {
            batchHeader = new BatchHeader(node, eventSize, 666, channel,
                                          sequenceNumber++);
            logicalSegment = eventChannel.appendSegmentFor(batchHeader);
            assertNotNull(logicalSegment);
            assertEquals(offset, logicalSegment.offset);
            assertEquals(position, logicalSegment.position);
            assertEquals(maxSegmentSize * 2, logicalSegment.segment.getPrefix());
            eventChannel.append(batchHeader, offset, logicalSegment.segment);
            position += eventSize;
        }
    }
}
