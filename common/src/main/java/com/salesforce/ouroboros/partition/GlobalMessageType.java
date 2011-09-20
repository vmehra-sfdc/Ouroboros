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
package com.salesforce.ouroboros.partition;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard.Member;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum GlobalMessageType implements GlobalDispatch {
    ADVERTISE_CHANNEL_BUFFER {
        @Override
        public void transition(Switchboard switchboard, Member member,
                               Node sender, Serializable payload, long time) {
            switchboard.discover(sender);
            member.dispatch(this, sender, payload, time);
        }

    },
    ADVERTISE_CONSUMER {

        @Override
        void transition(Switchboard switchboard, Member member, Node sender,
                        Serializable payload, long time) {
            switchboard.discover(sender);
            member.dispatch(this, sender, payload, time);
        }

    },
    ADVERTISE_PRODUCER {

        @Override
        void transition(Switchboard switchboard, Member member, Node sender,
                        Serializable payload, long time) {
            switchboard.discover(sender);
            member.dispatch(this, sender, payload, time);
        }

    },
    DISCOVERY_COMPLETE {
        @Override
        public void dispatch(Switchboard switchboard, Node sender,
                             Serializable payload, long time) {
        }

        @Override
        void transition(Switchboard switchboard, Member member, Node sender,
                        Serializable payload, long time) {
            switchboard.stabilized();

        }
    };
    abstract void transition(Switchboard switchboard, Member member,
                             Node sender, Serializable payload, long time);

    public void dispatch(Switchboard switchboard, Node sender,
                         Serializable payload, long time) {
        switchboard.dispatch(this, sender, payload, time);
    }
}