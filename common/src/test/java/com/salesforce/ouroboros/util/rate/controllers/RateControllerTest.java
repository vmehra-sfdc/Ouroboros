package com.salesforce.ouroboros.util.rate.controllers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
import junit.framework.TestCase;

import com.salesforce.ouroboros.util.rate.Predicate;

/**
 * 
 * @author hhildebrand
 * 
 */
public class RateControllerTest extends TestCase {
    public void testRateController() {
        Predicate predicate = mock(Predicate.class);
        RateController controller = new RateController(predicate, 0.01, 1, 10);
        controller.setTarget(1);

        controller.setSampleRate(0);
        for (int responseTime : new int[] { 10, 10, 10, 10, 10, 10, 1 }) {
            controller.sample(responseTime);
        }
        for (int i = 0; i < 25; i++) {
            controller.sample(0);
        }

        controller.sample(0);
        controller.sample(0);
        controller.sample(0);
        controller.sample(1);
        controller.sample(1);
        controller.sample(1);
        controller.sample(1);
        controller.sample(0);
        controller.sample(0);
        controller.sample(0);
        verify(predicate).setTargetRate(0.5);
        verify(predicate).setTargetRate(0.25);
        verify(predicate).setTargetRate(0.125);
        verify(predicate).setTargetRate(0.0625);
        verify(predicate).setTargetRate(0.03125);
        verify(predicate).setTargetRate(0.015625);
        verify(predicate).setTargetRate(0.01);
        verify(predicate).setTargetRate(0.51);
        verify(predicate, times(2)).setTargetRate(1.0);
        verifyNoMoreInteractions(predicate);
    }
}
