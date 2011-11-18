package com.salesforce.ouroboros.partition;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;

public enum LeaderNotification implements MemberDispatch{
    NOTIFY_PRODUCER_LEADER, NOTIFY_CONSUMER_LEADER;

    @Override
    public void dispatch(Switchboard switchboard, Node sender,
                         Serializable[] arguments, long time) {
        switchboard.dispatchToMember(this, sender, arguments, time);
    }
}
