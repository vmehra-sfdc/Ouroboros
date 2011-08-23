package com.salesforce.ouroboros.spindle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

public class ReplicatingAppender extends AbstractAppender implements
        CommunicationsHandler {
    private static final Logger log          = Logger.getLogger(ReplicatingAppender.class.getCanonicalName());

    private final EventChannel  eventChannel;
    private final ByteBuffer    offsetBuffer = ByteBuffer.allocate(8);

    public ReplicatingAppender(EventChannel eventChannel) {
        this.eventChannel = eventChannel;
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.AbstractAppender#commit()
     */
    @Override
    protected void commit() {
        eventChannel.append(header, offset);
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.spindle.AbstractAppender#headerRead(java.nio.channels.SocketChannel)
     */
    @Override
    protected void headerRead(SocketChannel channel) {
        if (Replicator.log.isLoggable(Level.FINER)) {
            Replicator.log.finer(String.format("Header read, header=%s", header));
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
     * @see com.salesforce.ouroboros.spindle.Appender#readOffset(java.nio.channels.SocketChannel)
     */
    @Override
    protected void readOffset(SocketChannel channel) {
        try {
            channel.read(offsetBuffer);
        } catch (IOException e) {
            log.log(Level.WARNING, "Exception during offset read", e);
            return;
        }
        if (!offsetBuffer.hasRemaining()) {
            offsetBuffer.flip();
            offset = position = offsetBuffer.getLong();
            if (log.isLoggable(Level.FINER)) {
                log.finer(String.format("Offset read, offset=%s", offset));
            }
            state = State.READ_HEADER;
            readHeader(channel);
        }
    }
}