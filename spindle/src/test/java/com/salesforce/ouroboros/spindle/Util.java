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
package com.salesforce.ouroboros.spindle;

import static junit.framework.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author hhildebrand
 * 
 */
public class Util {
    public static interface Condition {
        boolean value();
    }

    public static Object accessField(String fieldName, Object target)
                                                                     throws SecurityException,
                                                                     NoSuchFieldException,
                                                                     IllegalArgumentException,
                                                                     IllegalAccessException {
        Field field;
        try {
            field = target.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superClass = target.getClass().getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessField(fieldName, target, superClass);
        }
        field.setAccessible(true);
        return field.get(target);
    }

    public static Object accessField(String fieldName, Object target,
                                     Class<?> targetClass)
                                                          throws SecurityException,
                                                          NoSuchFieldException,
                                                          IllegalArgumentException,
                                                          IllegalAccessException {
        Field field;
        try {
            field = targetClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superClass = targetClass.getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessField(fieldName, target, superClass);
        }
        field.setAccessible(true);
        return field.get(target);
    }

    public static Method accessMethod(String methodName, Object[] arguments,
                                      Class<?> targetClass)
                                                           throws SecurityException,
                                                           NoSuchMethodException {
        Method method;

        List<Class<?>> argClasses = new ArrayList<Class<?>>();
        try {
            method = targetClass.getDeclaredMethod(methodName,
                                                   argClasses.toArray(new Class<?>[0]));
        } catch (NoSuchMethodException e) {
            Class<?> superClass = targetClass.getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessMethod(methodName, arguments, superClass);
        }
        return method;
    }

    public static Method accessMethod(String methodName, Object[] arguments,
                                      Object target) throws SecurityException,
                                                    NoSuchMethodException {
        Method method;

        List<Class<?>> argClasses = new ArrayList<Class<?>>();
        try {
            method = target.getClass().getDeclaredMethod(methodName,
                                                         argClasses.toArray(new Class<?>[0]));
        } catch (NoSuchMethodException e) {
            Class<?> superClass = target.getClass().getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessMethod(methodName, arguments, superClass);
        }
        return method;
    }

    public static void waitFor(String reason, Condition condition,
                               long timeout, long interval)
                                                           throws InterruptedException {
        long target = System.currentTimeMillis() + timeout;
        while (!condition.value()) {
            if (target < System.currentTimeMillis()) {
                fail(reason);
            }
            Thread.sleep(interval);
        }
    }
}
