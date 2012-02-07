/**
 */
package com.salesforce.ouroboros.producer.spinner;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import com.salesforce.ouroboros.Batch;
import com.salesforce.ouroboros.BatchHeader;
import com.salesforce.ouroboros.Event;
import com.salesforce.ouroboros.Node;

/**
 * @author hhildebrand
 * 
 */
public class TestBatch {

    @Test
    public void testRendering() {
        final String[] events = new String[] { "Give me Slack",
                "or give me food", "or kill me" };
        ByteBuffer[] payloads = new ByteBuffer[] {
                ByteBuffer.wrap(events[0].getBytes()),
                ByteBuffer.wrap(events[1].getBytes()),
                ByteBuffer.wrap(events[2].getBytes()) };
        final int[] crc32 = new int[] { Event.crc32(events[0].getBytes()),
                Event.crc32(events[1].getBytes()),
                Event.crc32(events[2].getBytes()) };
        Batch batch = new Batch(new Node(0), UUID.randomUUID(), 0L,
                                Arrays.asList(payloads));
        ByteBuffer buffer = batch.batch;
        BatchHeader.readFrom(buffer);
        for (int i = 0; i < 3; i++) {
            Event event = Event.readFrom(buffer);
            byte[] buf = new byte[events[i].getBytes().length];
            event.getPayload().get(buf);
            String payload = new String(buf);
            assertEquals(events[i].getBytes().length, event.size());
            assertEquals(crc32[i], event.getCrc32());
            assertTrue("event failed CRC check", event.validate());
            assertEquals(String.format("unexpected event content '%s'", payload),
                         events[i], payload);
        }
        assertEquals(0, buffer.remaining());
    }
}
