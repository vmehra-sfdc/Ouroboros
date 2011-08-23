package com.salesforce.ouroboros.spindle;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Appender extends AbstractAppender {
    private static final Logger   log = Logger.getLogger(Appender.class.getCanonicalName());

    private final Bundle          bundle;
    private volatile EventChannel eventChannel;

    public Appender(Bundle bundle) {
        this.bundle = bundle;
    }

    @Override
    protected void commit() {
        eventChannel.append(header, offset, segment);
    }

    @Override
    protected void headerRead(SocketChannel channel) {
        eventChannel = bundle.eventChannelFor(header);
        if (log.isLoggable(Level.FINER)) {
            log.finer(String.format("Header read, header=%s", header));
        }
        if (eventChannel == null) {
            log.warning(String.format("No existing event channel for: %s",
                                      header));
            state = State.DEV_NULL;
            segment = null;
            devNull = ByteBuffer.allocate(header.size());
            devNull(channel);
            return;
        }
        segment = eventChannel.segmentFor(header);
        offset = position = eventChannel.nextOffset();
        remaining = header.size();
        if (eventChannel.isDuplicate(header)) {
            state = State.DEV_NULL;
            segment = null;
            devNull = ByteBuffer.allocate(header.size());
            devNull(channel);
        } else {
            writeHeader();
            append(channel);
        }
    }
}