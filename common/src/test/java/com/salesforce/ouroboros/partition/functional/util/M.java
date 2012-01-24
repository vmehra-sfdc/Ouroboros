package com.salesforce.ouroboros.partition.functional.util;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;

public class M implements Member {
    public final Node        node;
    public volatile boolean  stabilized = false;
    public final Switchboard switchboard;

    M(Node n, Switchboard s) {
        node = n;
        switchboard = s;
        switchboard.setMember(this);
    }

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(node,
                                         DiscoveryMessage.ADVERTISE_CONSUMER));
    }

    @Override
    public void destabilize() {
        stabilized = false;
    }

    @Override
    public void dispatch(BootstrapMessage type, Node sender,
                         Serializable[] arguments, long time) {
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {

    }

    @Override
    public void dispatch(DiscoveryMessage type,
                         com.salesforce.ouroboros.Node sender,
                         Serializable[] arguments, long time) {

    }

    @Override
    public void dispatch(FailoverMessage type, Node sender,
                         Serializable[] arguments, long time) {
    }

    @Override
    public void dispatch(WeaverRebalanceMessage type, Node sender,
                         Serializable[] arguments, long time) {

    }

    @Override
    public void stabilized() {
        stabilized = true;
    }

    @Override
    public void becomeInactive() {
        // TODO Auto-generated method stub

    }

}