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
package com.salesforce.ouroboros.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simple coordinator for a group of asynchronous systems that need to
 * coordinated. The rendezvous is composed of a required number of participants
 * and an action to run when the required number of participants have met. The
 * rendezvous may optionally be scheduled for cancellation, providing a timeout
 * action to take if the rendezvous is cancelled due to timeout. The rendezvous
 * instance maintains this scheduled cancellation, clearing it if the rendezvous
 * is met or cancelled through other means.
 * 
 * @author hhildebrand
 * 
 */
public class Rendezvous {
    private final Runnable     action;
    private final Runnable     cancellationAction;
    private boolean            cancelled = false;
    private int                count;
    private final Object       mutex     = new Object();
    private final int          parties;
    private ScheduledFuture<?> scheduled;

    public Rendezvous(int parties, Runnable action,
                      final Runnable cancellationAction) {
        this.parties = parties;
        this.action = action;
        this.cancellationAction = cancellationAction;
        count = parties;
    }

    /**
     * Cancel the rendezvous. If the rendezvous has not been previously
     * cancelled, then run the cancellation action.
     */
    public void cancel() {
        synchronized (mutex) {
            if (count != 0 && !cancelled) {
                cancelled = true;
                if (scheduled != null) {
                    scheduled.cancel(true);
                }
                scheduled = null;
                if (cancellationAction != null) {
                    cancellationAction.run();
                }
            }
        }
    }

    /**
     * @return the number of parties that have made it past the rendezvous point
     */
    public int getCount() {
        synchronized (mutex) {
            return count;
        }
    }

    /**
     * @return the number of parties required to complete the rendezvous
     */
    public int getParties() {
        return parties;
    }

    /**
     * @return whether the rendezvous has been cancelled.
     */
    public boolean isCancelled() {
        synchronized (mutex) {
            return cancelled;
        }
    }

    /**
     * @return true if the number of parties in the rendezvous have met
     */
    public boolean isMet() {
        synchronized (mutex) {
            return count == 0;
        }
    }

    /**
     * Meet at the rendezvous point. If the number of parties has been met, then
     * run the rendezvous action.
     * 
     * @throws IllegalStateException
     *             - if the number of parties has already been met
     * @throws BrokenBarrierException
     *             - if the rendezvous has been cancelled.
     */
    public void meet() throws BrokenBarrierException {
        boolean run = false;
        synchronized (mutex) {
            if (count == 0) {
                throw new IllegalStateException("All parties have rendezvoused");
            }
            if (cancelled) {
                throw new BrokenBarrierException();
            }
            if (--count == 0) {
                if (scheduled != null) {
                    scheduled.cancel(true);
                }
                scheduled = null;
                run = true;
            }
        }
        if (run) {
            if (action != null) {
                action.run();
            }
        }
    }

    /**
     * @param n
     * @throws BrokenBarrierException
     */
    public void meet(int n) throws BrokenBarrierException {
        boolean run = false;
        synchronized (mutex) {
            if (count == 0) {
                throw new IllegalStateException("All parties have rendezvoused");
            }
            if (cancelled) {
                throw new BrokenBarrierException();
            }
            count -= n;
            if (count == 0) {
                if (scheduled != null) {
                    scheduled.cancel(true);
                }
                scheduled = null;
                run = true;
            }
        }
        if (run) {
            if (action != null) {
                action.run();
            }
        }
    }

    /**
     * Schedule a cancellation of the rendezvous. The scehduled cancellation is
     * tracked and maintained by the receiver.
     * 
     * @param timeout
     * @param unit
     * @param timer
     *            - the timer to schedule the cancellation
     */
    public void scheduleCancellation(long timeout, TimeUnit unit,
                                     ScheduledExecutorService timer) {
        synchronized (mutex) {
            if (scheduled != null) {
                throw new IllegalStateException(
                                                "Cancellation has already been scheduled");
            }
            if (cancelled) {
                throw new IllegalStateException(
                                                "Rendezvous has already been cancelled");
            }
            if (count == 0) {
                return;
            }
            scheduled = timer.schedule(new Runnable() {
                @Override
                public void run() {
                    cancel();
                }
            }, timeout, unit);
        }
    }
}
