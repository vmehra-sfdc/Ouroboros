package com.salesforce.ouroboros.util.rate.controllers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;

import com.salesforce.ouroboros.util.rate.Predicate;

/**
 * 
 * @author hhildebrand
 * 
 */
public class RateControllerTest {
    @Test
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
