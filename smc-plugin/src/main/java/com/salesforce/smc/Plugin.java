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

/*
 * Copyright 2001-2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

import net.sf.smc.Smc;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * @author hhildebrand
 * 
 *         Goal which touches a timestamp file.
 * 
 * @goal generate
 * 
 * @phase generate-sources
 */
public class Plugin extends AbstractMojo {
    /**
     * Location of the build directory.
     * 
     * @parameter expression="${project.build.directory}"
     * @required
     */
    private File    buildDirectory;

    /**
     * DebugLevel. 0, 1: Adds debug output messages to the generated code. 0
     * produces output messages which signal when the FSM has exited a state,
     * entered a state, entered and exited a transition. 1 includes the 0 output
     * and addition output for entering and exiting state Exit and Entry
     * actions.
     * 
     * @parameter
     */
    private int     debugLevel      = -1;

    /**
     * FSM verbose output
     * 
     * @parameter
     */
    private boolean fsmVerbose      = false;

    /**
     * Generic collections. May be used only with target languages csharp, java
     * or vb and reflection. Causes SMC to use generic collections for
     * reflection.
     * 
     * @parameter
     */
    private boolean generic         = true;

    /**
     * Produce DOT graph output.
     * 
     * @parameter
     */
    private boolean graph           = false;

    /**
     * Graph level. Specifies how much detail to place into the DOT file. Level
     * 0 is the least detail and 2 is the greatest.
     * 
     * @parameter
     */
    private int     graphLevel      = -1;

    /**
     * Location of the project home directory.
     * 
     * @parameter expression="${project.home.directory}"
     * @required
     */
    private File    projectDirectory;

    /**
     * Reflection. May be used only with target languages csharp, groovy, java,
     * lua, perl, php, python, ruby, scala, tcl and vb. Causes SMC to generate a
     * getTransitions method, allowing applications to query the available
     * transitions for the current state.
     * 
     * @parameter
     */
    private boolean reflection      = false;

    /**
     * Serialization. Generate unique integer IDs for each state. These IDs can
     * be used when persisting an FSM.
     * 
     * @parameter
     */
    private boolean serial          = true;

    /**
     * State machine files source directory, relative to the project root.
     * 
     * @parameter
     */
    private String  smDirectory     = "sm";

    /**
     * May be used only with the java, groovy, scala, vb and csharp target
     * languages. Causes SMC to:
     * <ul>
     * <li>Java: add the synchronized keyword to the transition method
     * declarations.</li>
     * <li>Groovy: add the synchronized keyword to the transition method
     * declarations.</li>
     * <li>Scala: add the synchronized keyword to the transition method
     * declarations.</li>
     * <li>VB.net: encapsulate the transition method's body in a SyncLock Me,
     * End SyncLock block.</li>
     * <li>C#: encapsulate the transition method's body in a lock(this) {...}
     * block.</li>
     * </ul>
     * 
     * @parameter
     */
    private boolean sync            = false;

    /**
     * Produce HTML table output.
     * 
     * @parameter
     */
    private boolean table           = false;

    /**
     * Target language
     * 
     * @parameter
     */
    private String  target          = "java";

    /**
     * Generated source directory, relative to the project build directory
     * 
     * @parameter
     */
    private String  targetDirectory = "generated-sources/sm";

    /**
     * Verbose output.
     * 
     * @parameter
     */
    private boolean verbose         = false;

    public Plugin() {
        super();
    }

    public Plugin(File buildDirectory, int debugLevel, boolean generic,
                  boolean graph, int graphLevel, File projectDirectory,
                  boolean reflection, boolean serial, String smDirectory,
                  boolean sync, boolean table, String target,
                  String targetDirectory, boolean verbose, boolean fsmVerbose) {
        super();
        this.buildDirectory = buildDirectory;
        this.debugLevel = debugLevel;
        this.generic = generic;
        this.graph = graph;
        this.graphLevel = graphLevel;
        this.projectDirectory = projectDirectory;
        this.reflection = reflection;
        this.serial = serial;
        this.smDirectory = smDirectory;
        this.sync = sync;
        this.table = table;
        this.target = target;
        this.targetDirectory = targetDirectory;
        this.verbose = verbose;
        this.fsmVerbose = fsmVerbose;
    }

    @Override
    public void execute() throws MojoExecutionException {
        ArrayList<String> commonArgs = new ArrayList<String>();

        commonArgs.add("-return");
        File targetDir = new File(buildDirectory, targetDirectory);
        targetDir.mkdirs();

        commonArgs.add("-d");
        commonArgs.add(targetDir.getAbsolutePath());

        File srcDir = new File(projectDirectory, smDirectory);

        ArrayList<String> sources = new ArrayList<String>();
        for (File source : srcDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".sm");
            }
        })) {
            sources.add(source.getAbsolutePath());
        }

        ArrayList<String> args = new ArrayList<String>(commonArgs);

        switch (debugLevel) {
            case 0:
                args.add("-g0");
                break;
            case 1:
                args.add("-g1");
                break;
            default:
        }

        if (verbose) {
            args.add("-verbose");
        }

        if (fsmVerbose) {
            args.add("-vverbose");
        }
        if (sync) {
            args.add("-sync");
        }

        if (serial) {
            args.add("-serial");
        }

        if (reflection) {
            args.add("-reflection");
            if (generic) {
                args.add("-generic");
            }
        }
        args.add("-" + target);
        args.addAll(sources);

        // generate FSM source
        Smc.main(args.toArray(new String[0]));

        if (graph) {
            args = new ArrayList<String>(commonArgs);
            args.add("-graph");
            switch (graphLevel) {
                case 0:
                    args.add("-gLevel");
                    args.add("0");
                    break;
                case 1:
                    args.add("-gLevel");
                    args.add("1");
                    break;
                default:
            }
            args.addAll(sources);

            // Generate graphs
            Smc.main(args.toArray(new String[0]));
        }

        if (table) {
            args = new ArrayList<String>(commonArgs);
            args.add("-table");
            args.addAll(sources);

            // Generate graphs
            Smc.main(args.toArray(new String[0]));
        }
    }
}
