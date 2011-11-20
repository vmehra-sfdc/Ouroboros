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
package com.salesforce.ouroboros.spindle.messages;

import java.io.Serializable;
import java.util.logging.Logger;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.MemberDispatch;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.spindle.Coordinator;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum ReplicatorMessage implements MemberDispatch {
    OPEN_REPLICATORS, REPLICATORS_ESTABLISHED;

    @Override
    public void dispatch(Switchboard switchboard, Node sender,
                         Serializable[] arguments, long time) {
        if (!(switchboard.getMember() instanceof Coordinator)) {
            log.warning(String.format("ReplicatorMessage %s must be targeted at weaver coordinator, not %s",
                                      this, switchboard.getMember()));
        }
        Coordinator coordinator = (Coordinator) switchboard.getMember();
        coordinator.dispatch(this, sender, arguments, time);
    }

    private final static Logger log = Logger.getLogger(ReplicatorMessage.class.getCanonicalName());
}
