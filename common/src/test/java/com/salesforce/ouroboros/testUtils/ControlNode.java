package com.salesforce.ouroboros.testUtils;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.views.View;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;

public class ControlNode extends NodeData {
    static final Logger log = Logger.getLogger(ControlNode.class.getCanonicalName());

    public int                 cardinality;
    public CountDownLatch      latch;

    public ControlNode(Heartbeat hb, Controller controller) {
        super(hb, controller);
    }

    @Override
    protected void partitionNotification(View partition, int leader) {
        log.fine("Partition notification: " + partition);
        super.partitionNotification(partition, leader);
        if (partition.isStable() && partition.cardinality() == cardinality) {
            latch.countDown();
        }
    }
}