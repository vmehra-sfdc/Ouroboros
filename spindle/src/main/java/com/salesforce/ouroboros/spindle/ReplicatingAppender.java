package com.salesforce.ouroboros.spindle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatingAppender extends Appender {
    private static final Logger log          = LoggerFactory.getLogger(ReplicatingAppender.class);
    private final ByteBuffer    offsetBuffer = ByteBuffer.allocate(8);

    public ReplicatingAppender(Bundle bundle, Producer producer) {
        super(bundle, producer);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Appender#initialRead(java.nio.channels.SocketChannel)
     */
    @Override
    protected void initialRead(SocketChannel channel) {
        state = State.READ_OFFSET;
        offsetBuffer.clear();
        readOffset(channel);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Appender#readHeader(java.nio.channels.SocketChannel)
     */
    @Override
    protected void readHeader(SocketChannel channel) {
        boolean read;
        try {
            read = header.read(channel);
        } catch (IOException e) {
            log.error("Exception during header read", e);
            return;
        }
        if (read) {
            eventChannel = bundle.eventChannelFor(header);
            if (eventChannel == null) {
                log.info(String.format("No existing event channel for: %s",
                                       header));
                state = State.DEV_NULL;
                segment = null;
                devNull = ByteBuffer.allocate(header.size());
                devNull(channel);
                return;
            }
            segment = eventChannel.segmentFor(offset);
            remaining = header.size();
            if (!eventChannel.isNextAppend(offset)) {
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

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.Appender#readOffset(java.nio.channels.SocketChannel)
     */
    @Override
    protected void readOffset(SocketChannel channel) {
        try {
            channel.read(offsetBuffer);
        } catch (IOException e) {
            log.error("Exception during offset read", e);
            return;
        }
        if (!offsetBuffer.hasRemaining()) {
            offset = position = offsetBuffer.getLong();
            state = State.READ_HEADER;
            readHeader(channel);
        }
    }

}
