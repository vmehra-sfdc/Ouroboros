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
package com.salesforce.ouroboros.consumer;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;

/**
 * 
 * The event consumer of Ouroboros.
 * 
 * @author hhildebrand
 * 
 */
public class Consumer implements Member {

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#advertise()
     */
    @Override
    public void advertise() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#becomeInactive()
     */
    @Override
    public void becomeInactive() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#destabilize()
     */
    @Override
    public void destabilize() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.messages.BootstrapMessage, com.salesforce.ouroboros.Node, java.io.Serializable[], long)
     */
    @Override
    public void dispatch(BootstrapMessage type, Node sender,
                         Serializable[] arguments, long time) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.messages.ChannelMessage, com.salesforce.ouroboros.Node, java.io.Serializable[], long)
     */
    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.messages.DiscoveryMessage, com.salesforce.ouroboros.Node, java.io.Serializable[], long)
     */
    @Override
    public void dispatch(DiscoveryMessage type, Node sender,
                         Serializable[] arguments, long time) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.messages.FailoverMessage, com.salesforce.ouroboros.Node, java.io.Serializable[], long)
     */
    @Override
    public void dispatch(FailoverMessage type, Node sender,
                         Serializable[] arguments, long time) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#dispatch(com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage, com.salesforce.ouroboros.Node, java.io.Serializable[], long)
     */
    @Override
    public void dispatch(WeaverRebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.salesforce.ouroboros.partition.Switchboard.Member#stabilized()
     */
    @Override
    public void stabilized() {
        // TODO Auto-generated method stub

    }

    /**
     * 
     */
    public void destabilizePartition() {
        // TODO Auto-generated method stub

    }

}
