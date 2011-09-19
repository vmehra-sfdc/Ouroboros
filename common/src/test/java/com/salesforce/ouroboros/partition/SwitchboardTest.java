/*               
 * Copyright (C) 2008-2010 Paolo Boldi, Massimo Santini and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

package com.salesforce.ouroboros.partition;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.comms.MessageConnection;
import org.smartfrog.services.anubis.partition.views.BitView;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard.Member;

/**
 * 
 * @author hhildebrand
 * 
 */
public class SwitchboardTest {

    @Test
    public void testBasic() {
        Node node = new Node(0, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        Executor exec = mock(Executor.class);

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        verify(member).setSwitchboard(switchboard);
        switchboard.start();
        verify(partition).register(isA(PartitionNotification.class));
        switchboard.terminate();
        verify(partition).deregister(isA(PartitionNotification.class));
    }

    @Test
    public void testDestabilize() {
        Executor exec = mock(Executor.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.partitionEvent(view, 0);
        switchboard.discover(node);
        switchboard.discover(testNode);

        Node testNode2 = new Node(2, 0, 0);
        when(partition.connect(testNode2.processId)).thenReturn(connection);

        view.add(testNode2.processId);
        view.remove(testNode.processId);
        switchboard.partitionEvent(view, 0);
        assertEquals(State.UNSTABLE, switchboard.getState());
        verify(member).destabilize();
        view.stablize();
        switchboard.partitionEvent(view, 0);

        switchboard.discover(node);
        switchboard.discover(testNode2);

        verify(connection).sendObject(isA(RingMessage.class));
        assertEquals(2, switchboard.getMembers().size());
        assertTrue(switchboard.getMembers().contains(node));
        assertTrue(switchboard.getMembers().contains(testNode2));
        assertFalse(switchboard.getMembers().contains(testNode));

        assertEquals(1, switchboard.getNewMembers().size());
        assertTrue(switchboard.getNewMembers().contains(testNode2));

        assertEquals(1, switchboard.getDeadMembers().size());
        assertTrue(switchboard.getDeadMembers().contains(testNode));
    }

    @Test
    public void testDiscovery() {
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Executor exec = mock(Executor.class);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                RingMessage message = (RingMessage) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE,
                             message.wrapped.type);
                return null;
            }
        }).when(connection).sendObject(isA(RingMessage.class));

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.partitionEvent(view, 0);
        switchboard.discover(node);
        switchboard.discover(testNode);
        verify(connection).sendObject(isA(RingMessage.class));
        assertEquals(2, switchboard.getMembers().size());
        assertTrue(switchboard.getMembers().contains(node));
        assertTrue(switchboard.getMembers().contains(testNode));
        assertEquals(2, switchboard.getNewMembers().size());
        assertTrue(switchboard.getNewMembers().contains(node));
        assertTrue(switchboard.getNewMembers().contains(testNode));
    }

    @Test
    public void testRingCast() {
        Executor exec = mock(Executor.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.members.add(node);
        switchboard.members.add(testNode);
        switchboard.partitionEvent(view, 0);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                RingMessage message = (RingMessage) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE,
                             message.wrapped.type);
                return null;
            }
        }).when(connection).sendObject(isA(RingMessage.class));

        switchboard.ringCast(new Message(node,
                                         GlobalMessageType.DISCOVERY_COMPLETE));
        verify(connection).sendObject(isA(RingMessage.class));

    }

    @Test
    public void testBroadcast() {
        Executor exec = mock(Executor.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection1 = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection1);
        MessageConnection connection2 = mock(MessageConnection.class);
        when(partition.connect(node.processId)).thenReturn(connection2);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.partitionEvent(view, 0);
        switchboard.members.add(node);
        switchboard.members.add(testNode);

        Answer<Void> answer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE, message.type);
                return null;
            }
        };
        doAnswer(answer).when(connection1).sendObject(isA(RingMessage.class));
        doAnswer(answer).when(connection2).sendObject(isA(RingMessage.class));

        switchboard.broadcast(new Message(node,
                                          GlobalMessageType.DISCOVERY_COMPLETE));
        verify(connection1).sendObject(isA(Message.class));
        verify(connection2).sendObject(isA(Message.class));

    }

    @Test
    public void testSend() {
        Executor exec = mock(Executor.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.stabilized();
        Message msg = new Message(node, GlobalMessageType.DISCOVERY_COMPLETE);
        switchboard.send(msg, testNode);
        verify(connection).sendObject(msg);
    }

    @Test
    public void testStabilize() {
        Executor exec = mock(Executor.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(member, node, partition, exec);
        switchboard.partitionEvent(view, 0);
        assertEquals(State.STABLE, switchboard.getState());
        verify(member).advertise();
    }
}
