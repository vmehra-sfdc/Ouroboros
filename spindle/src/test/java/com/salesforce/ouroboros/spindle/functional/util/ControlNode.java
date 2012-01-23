package com.salesforce.ouroboros.spindle.functional.util;

import java.util.concurrent.CountDownLatch;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.views.View;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;

public class ControlNode extends NodeData {
    public int            cardinality;
    public CountDownLatch latch;

    public ControlNode(Heartbeat hb, Controller controller) {
        super(hb, controller);
    }

    @Override
    protected void partitionNotification(View partition, int leader) {
        super.partitionNotification(partition, leader);
        if (partition.isStable() && partition.cardinality() == cardinality) {
            latch.countDown();
        }
    }
}