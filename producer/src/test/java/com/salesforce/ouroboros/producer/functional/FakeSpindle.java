package com.salesforce.ouroboros.producer.functional;

import java.io.Serializable;
import java.net.InetSocketAddress;

import com.salesforce.ouroboros.ContactInformation;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;

public class FakeSpindle implements Member {
    int               PORT = 55555;
    final Switchboard switchboard;

    public FakeSpindle(Switchboard switchboard) {
        this.switchboard = switchboard;
        switchboard.setMember(this);
    }

    @Override
    public void advertise() {
        ContactInformation info = new ContactInformation(
                                                         new InetSocketAddress(
                                                                               "127.0.0.1",
                                                                               PORT++),
                                                         null, null);
        switchboard.ringCast(new Message(
                                         switchboard.getId(),
                                         DiscoveryMessage.ADVERTISE_CHANNEL_BUFFER,
                                         info, true));
    }

    @Override
    public void becomeInactive() {
    }

    @Override
    public void destabilize() {
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
    public void dispatch(DiscoveryMessage type, Node sender,
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
    }

}