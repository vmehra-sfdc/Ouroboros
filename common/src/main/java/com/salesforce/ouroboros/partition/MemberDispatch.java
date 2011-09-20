package com.salesforce.ouroboros.partition;

import java.io.Serializable;

import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard.Member;

public interface MemberDispatch {
    void dispatch(Member member, Node sender, Serializable payload, long time);
}
