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
package com.salesforce.ouroboros;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.partition.views.View;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum PartitionState {
    STABLE {
        @Override
        void next(PartitionState next, Switchboard switchboard, View view,
                  int leader) {
            switch (next) {
                case STABLE: {
                    if (log.isLoggable(Level.INFO)) {
                        log.info(String.format("Stable view received while in the stable state: %s, leader: %s",
                                               view, leader));
                    }
                    break;
                }
                case UNSTABLE: {
                    switchboard.destabilize(view, leader);
                }
            }
        }

    },
    UNSTABLE {
        @Override
        void next(PartitionState next, Switchboard switchboard, View view,
                  int leader) {
            switch (next) {
                case UNSTABLE: {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest(String.format("Untable view received while in the unstable state: %s, leader: %s",
                                                 view, leader));
                    }
                    break;
                }
                case STABLE: {
                    switchboard.stabilize(view, leader);
                }
            }
        }

    };
    private final static Logger log = Logger.getLogger(PartitionState.class.getCanonicalName());

    abstract void next(PartitionState next, Switchboard switchboard, View view,
                       int leader);
}