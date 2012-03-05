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

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import com.fasterxml.uuid.UUIDType;
import com.fasterxml.uuid.impl.NameBasedGenerator;

/**
 * 
 * @author hhildebrand
 * 
 */
final public class Utils {
    private final static NameBasedGenerator UUID_GENERATOR;

    static {
        MessageDigest digester;
        try {
            digester = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                                            "Cannot get SHA-1 digester instance",
                                            e);
        }
        UUID_GENERATOR = new NameBasedGenerator(null, digester,
                                                UUIDType.NAME_BASED_SHA1);
    }

    public static void delete(File dirOrFile) {
        if (dirOrFile.isDirectory()) {
            deleteDirectory(dirOrFile);
        } else {
            dirOrFile.delete();
        }
    }

    public static void deleteDirectory(File dir) {
        File[] listing = dir.listFiles();
        if (listing != null) {
            for (File f : listing) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    public static boolean isClose(IOException ioe) {
        return ioe instanceof ClosedChannelException
               || "Broken pipe".equals(ioe.getMessage())
               || "Connection reset by peer".equals(ioe.getMessage());
    }

    public static long point(UUID id) {
        return id.getLeastSignificantBits() ^ id.getMostSignificantBits();
    }

    public static UUID toUUID(String string) {
        return UUID_GENERATOR.generate(string);
    }
}
