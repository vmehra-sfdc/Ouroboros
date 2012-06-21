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
package com.salesforce.ouroboros.consumer.shuttle;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.Weft;
import com.salesforce.ouroboros.batch.BatchWriter;
import com.salesforce.ouroboros.util.Utils;

/**
 * @author hhildebrand
 * 
 */
public class Loom implements CommunicationsHandler, LoomInbound {
    private static Logger            log        = LoggerFactory.getLogger(Loom.class);
    private static int               MAX_AGENDA = 100;

    private SocketChannelHandler     handler;
    private boolean                  inError;
    private final ByteBuffer         weftBuffer;
    private final LoomInboundContext weftFsm    = new LoomInboundContext(this);
    private final BatchWriter<Weft>  agenda;

    public Loom(int agendaBufferCapacity, int weftBufferCapacity, Node self,
                Node partner) {
        weftBuffer = ByteBuffer.allocateDirect(weftBufferCapacity);
        agenda = new BatchWriter<>(Weft.BYTE_SIZE, agendaBufferCapacity, "Weft");
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#accept(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void accept(SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.consumer.shuttle.LoomOutbound#close()
     */
    @Override
    public void close() {
        handler.close();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#closing()
     */
    @Override
    public void closing() {
        agenda.close();
        weftFsm.close();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#connect(com.hellblazer.pinkie.SocketChannelHandler)
     */
    @Override
    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.consumer.shuttle.LoomOutbound#inError()
     */
    @Override
    public boolean inError() {
        return inError;
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.consumer.shuttle.LoomInbound#readPush()
     */
    @Override
    public boolean readPush() {
        while (fillBuffer()) {
            weftBuffer.flip();
            while (weftBuffer.remaining() >= Weft.BYTE_SIZE) {
                Weft thread = new Weft(weftBuffer);
                // spinner.acknowledge(thread);
            }
            weftBuffer.compact();
        }
        return false;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#readReady()
     */
    @Override
    public void readReady() {
        weftFsm.readReady();
    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.consumer.shuttle.LoomInbound#selectForRead()
     */
    @Override
    public void selectForRead() {
        handler.selectForRead();
    }

    private boolean fillBuffer() {
        int read;
        try {
            read = handler.getChannel().read(weftBuffer);
            if (read < 0) {
                if (log.isInfoEnabled()) {
                    log.info("closing channel");
                }
                inError = true;
                return false;
            } else if (weftBuffer.hasRemaining()) {
                // extra read attempt
                int plusRead = handler.getChannel().read(weftBuffer);
                if (plusRead < 0) {
                    if (log.isInfoEnabled()) {
                        log.info("closing channel");
                    }
                    inError = true;
                    return false;
                }
                read += plusRead;
            }
        } catch (IOException e) {
            if (!Utils.isClose(e)) {
                if (log.isInfoEnabled()) {
                    log.info("Closing batch acknowlegement");
                }
            }
            inError = true;
            return false;
        }
        return read != 0;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.CommunicationsHandler#writeReady()
     */
    @Override
    public void writeReady() {
        agenda.writeReady();
    }
}
