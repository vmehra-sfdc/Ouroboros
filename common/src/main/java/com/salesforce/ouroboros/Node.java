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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import com.salesforce.ouroboros.util.ConsistentHashFunction;

/**
 * Representation of the identity of a node. Each node has a unique process id.
 * Every node is hosted on a machine with a unique id, which in turn is mounted
 * in a rack with a unique id.
 * 
 * @author hhildebrand
 * 
 */
public class Node implements Comparable<Node>, Serializable {

    /**
     * Skip processes that share machines or rack ids
     * 
     */
    public class NodeSkipStrategy implements
            ConsistentHashFunction.SkipStrategy<Node> {
        @Override
        public boolean isSkippable(List<Node> previous, Node bucket) {
            for (Node node : previous) {
                if (node.machineId == machineId) {
                    return true;
                }
                if (node.rackId == rackId) {
                    return true;
                }
            }
            return false;
        }
    }

    public static final int BYTE_LENGTH = 4 + 4 + 4 + 4;
    private static final long serialVersionUID = 1L;

    public final int        capacity;
    public final int        machineId;
    public final int        processId;
    public final int        rackId;

    public Node(ByteBuffer buffer) {
        processId = buffer.getInt();
        machineId = buffer.getInt();
        rackId = buffer.getInt();
        capacity = buffer.getInt();
    }

    public Node(int processId, int machineId, int rackId) {
        this(processId, machineId, rackId, 1);
    }

    public Node(int processId, int machineId, int rackId, int capacity) {
        this.processId = processId;
        this.machineId = machineId;
        this.rackId = rackId;
        this.capacity = capacity;
    }

    @Override
    public int compareTo(Node node) {
        if (processId < node.processId) {
            return -1;
        }
        if (processId == node.processId) {
            return 0;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node)) {
            return false;
        }

        return ((Node) o).processId == processId;
    }

    @Override
    public int hashCode() {
        return processId;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(processId).putInt(machineId).putInt(rackId).putInt(capacity);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Node [processId=" + processId + ", machineId=" + machineId
               + ", rackId=" + rackId + ", capacity=" + capacity + "]";
    }
}
