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
package com.salesforce.smc;

import static junit.framework.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestPlugin {
    @Test
    public void testGenerate() throws Exception {
        File tempDir = File.createTempFile("smc", "generated");
        tempDir.delete();
        tempDir.deleteOnExit();
        String targetDirectory = "generated-sources/sm";
        Plugin plugin = new Plugin(tempDir, 2, true, true, 2, new File("."),
                                   true, true, "src/test/resources/sm", true,
                                   true, "java", targetDirectory, true, false);
        plugin.execute();
        File genDir = new File(tempDir, targetDirectory);
        assertTrue("TaskFSM DOT file not generated",
                   new File(genDir, "TaskFSM.dot").exists());
        assertTrue("TaskFSM HTML table not generated",
                   new File(genDir, "TaskFSM.html").exists());
        assertTrue("TaskFSM.java not generated",
                   new File(genDir, "TaskFSM.java").exists());

        assertTrue("TaskManagerFSM DOT file not generated",
                   new File(genDir, "TaskManagerFSM.dot").exists());
        assertTrue("TaskManagerFSM HTML table not generated",
                   new File(genDir, "TaskManagerFSM.html").exists());
        assertTrue("TaskManagerFSM.java not generated",
                   new File(genDir, "TaskManagerFSM.java").exists());
    }
}
