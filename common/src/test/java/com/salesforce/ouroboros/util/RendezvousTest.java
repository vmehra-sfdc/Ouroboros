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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * 
 * @author hhildebrand
 * 
 */
public class RendezvousTest {
    @SuppressWarnings("unchecked")
    @Test
    public void testCancellation() {
        final AtomicBoolean ran = new AtomicBoolean();
        Rendezvous rendezvous = new Rendezvous(2, new Runnable() {
            @Override
            public void run() {
                fail("completion action should not have run");
            }
        }, new Runnable() {
            @Override
            public void run() {
                ran.set(true);
            }
        });
        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture future = mock(ScheduledFuture.class);

        int delay = 1000;
        TimeUnit unit = TimeUnit.MILLISECONDS;

        when(
             timer.schedule(isA(Runnable.class), isA(Long.class),
                            isA(TimeUnit.class))).thenReturn(future);

        rendezvous.scheduleCancellation(delay, unit, timer);

        assertFalse(rendezvous.isCancelled());
        assertFalse(rendezvous.isMet());

        rendezvous.cancel();

        verify(future).cancel(true);
        assertTrue(ran.get());
        assertTrue(rendezvous.isCancelled());

        ran.set(false);
        rendezvous.cancel();
        assertFalse(ran.get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCountdown() throws Exception {
        final AtomicBoolean ran = new AtomicBoolean();

        Rendezvous rendezvous = new Rendezvous(2, new Runnable() {
            @Override
            public void run() {
                if (!ran.compareAndSet(false, true)) {
                    fail("Action expected to be run only once!");
                }
            }
        }, new Runnable() {
            @Override
            public void run() {
                fail("Cancellation action should not have run");
            }
        });

        ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture future = mock(ScheduledFuture.class);

        int delay = 1000;
        TimeUnit unit = TimeUnit.MILLISECONDS;

        when(
             timer.schedule(isA(Runnable.class), isA(Long.class),
                            isA(TimeUnit.class))).thenReturn(future);

        rendezvous.scheduleCancellation(delay, unit, timer);

        rendezvous.meet();
        assertFalse(ran.get());
        assertFalse(rendezvous.isMet());

        rendezvous.meet();
        assertTrue(ran.get());
        assertTrue(rendezvous.isMet());
        assertFalse(rendezvous.isCancelled());

        rendezvous.cancel();
        assertTrue(ran.get());
        assertTrue(rendezvous.isMet());
        assertFalse(rendezvous.isCancelled());
        try {
            rendezvous.meet();
            fail("Expected exception as all parties have met");
        } catch (IllegalStateException e) {
            // expected
        }
    }
}
