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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.PartitionNotification;
import org.smartfrog.services.anubis.partition.comms.MessageConnection;
import org.smartfrog.services.anubis.partition.util.NodeIdSet;
import org.smartfrog.services.anubis.partition.views.BitView;
import org.smartfrog.services.anubis.partition.views.View;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.SwitchboardContext.SwitchboardFSM;

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

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.start();
        verify(partition).register(isA(PartitionNotification.class));
        switchboard.terminate();
        verify(partition).deregister(isA(PartitionNotification.class));
    }

    @Test
    public void testDestabilize() {
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(view, 0);
        switchboard.discover(node);
        switchboard.discover(testNode);

        Node testNode2 = new Node(2, 0, 0);
        when(partition.connect(testNode2.processId)).thenReturn(connection);

        view.add(testNode2.processId);
        view.remove(testNode.processId);
        switchboard.partitionEvent(view, 0);
        assertEquals(SwitchboardFSM.Unstable, switchboard.getState());
        verify(member).destabilize();
        view.stablize();
        switchboard.partitionEvent(view, 0);

        switchboard.discover(node);
        switchboard.discover(testNode2);

        verify(connection).sendObject(isA(Message.class));
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
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE, message.type);
                return null;
            }
        }).when(connection).sendObject(isA(Message.class));

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(view, 0);
        switchboard.discover(node);
        switchboard.discover(testNode);
        verify(connection).sendObject(isA(Message.class));
        assertEquals(2, switchboard.getMembers().size());
        assertTrue(switchboard.getMembers().contains(node));
        assertTrue(switchboard.getMembers().contains(testNode));
        assertEquals(2, switchboard.getNewMembers().size());
        assertTrue(switchboard.getNewMembers().contains(node));
        assertTrue(switchboard.getNewMembers().contains(testNode));
    }

    @Test
    public void testRingCast() {
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

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.add(node);
        switchboard.add(testNode);
        switchboard.partitionEvent(view, 0);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE, message.type);
                return null;
            }
        }).when(connection).sendObject(isA(Message.class));

        switchboard.ringCast(new Message(node,
                                         GlobalMessageType.DISCOVERY_COMPLETE));
        verify(connection).sendObject(isA(Message.class));

    }

    @Test
    public void testBroadcast() {
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

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(view, 0);
        switchboard.add(node);
        switchboard.add(testNode);

        Answer<Void> answer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE, message.type);
                return null;
            }
        };
        doAnswer(answer).when(connection1).sendObject(isA(Message.class));
        doAnswer(answer).when(connection2).sendObject(isA(Message.class));

        switchboard.broadcast(new Message(node,
                                          GlobalMessageType.DISCOVERY_COMPLETE));
        verify(connection1).sendObject(isA(Message.class));
        verify(connection2).sendObject(isA(Message.class));

    }

    @Test
    public void testSend() {
        View v = mock(View.class);
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        NodeIdSet idSet = new NodeIdSet();
        when(v.toBitSet()).thenReturn(idSet);
        when(v.isStable()).thenReturn(true);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(v, 0);
        Message msg = new Message(testNode,
                                  GlobalMessageType.DISCOVERY_COMPLETE);
        switchboard.send(msg, testNode);
        verify(connection).sendObject(msg);
    }

    @Test
    public void testAdvertise() {
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

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(view, 0);
        assertEquals(SwitchboardFSM.Advertising, switchboard.getState());
        verify(member).advertise();
    }

    @Test
    public void testStabilize() {
        Node node = new Node(0, 0, 0);
        Node testNode = new Node(1, 0, 0);
        Partition partition = mock(Partition.class);
        Member member = mock(Member.class);
        MessageConnection connection = mock(MessageConnection.class);
        when(partition.connect(testNode.processId)).thenReturn(connection);
        Answer<Void> answer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                assertEquals(GlobalMessageType.DISCOVERY_COMPLETE, message.type);
                return null;
            }
        };
        doAnswer(answer).when(connection).sendObject(isA(Message.class));
        BitView view = new BitView();
        view.add(node.processId);
        view.add(testNode.processId);
        view.stablize();

        Switchboard switchboard = new Switchboard(node, partition);
        switchboard.setMember(member);
        switchboard.partitionEvent(view, 0);
        switchboard.discover(node);
        switchboard.discover(testNode);
        switchboard.dispatch(GlobalMessageType.DISCOVERY_COMPLETE, node, null, -1);
        assertEquals(SwitchboardFSM.Stable, switchboard.getState());
        verify(member).stabilized();
        verify(connection).sendObject(isA(Message.class));
    }
}
