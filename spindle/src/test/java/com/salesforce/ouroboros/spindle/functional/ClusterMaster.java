package com.salesforce.ouroboros.spindle.functional;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Message;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.partition.Switchboard.Member;
import com.salesforce.ouroboros.partition.messages.BootstrapMessage;
import com.salesforce.ouroboros.partition.messages.ChannelMessage;
import com.salesforce.ouroboros.partition.messages.DiscoveryMessage;
import com.salesforce.ouroboros.partition.messages.FailoverMessage;
import com.salesforce.ouroboros.partition.messages.WeaverRebalanceMessage;

public class ClusterMaster implements Member {
    CountDownLatch    openLatch;
    final Switchboard switchboard;

    public ClusterMaster(Switchboard switchboard) {
        this.switchboard = switchboard;
        switchboard.setMember(this);
    }

    @Override
    public void advertise() {
        switchboard.ringCast(new Message(switchboard.getId(),
                                         DiscoveryMessage.ADVERTISE_NOOP));
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
        if (openLatch == null) {
            return;
        }
        switch (type) {
            case PRIMARY_OPENED:
            case MIRROR_OPENED:
                openLatch.countDown();
                break;
            default:
        }
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

    public boolean open(UUID channel, long timeout, TimeUnit unit)
                                                                  throws InterruptedException {
        openLatch = new CountDownLatch(2);
        switchboard.ringCast(new Message(switchboard.getId(),
                                         ChannelMessage.OPEN,
                                         new Serializable[] { channel }));
        return openLatch.await(timeout, unit);
    }

    @Override
    public void stabilized() {
    }

}