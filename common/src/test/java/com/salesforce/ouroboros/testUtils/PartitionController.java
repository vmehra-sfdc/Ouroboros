package com.salesforce.ouroboros.testUtils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;
import org.smartfrog.services.anubis.partition.wire.security.WireSecurity;

import com.hellblazer.pinkie.SocketOptions;

public class PartitionController extends Controller {
    public int            cardinality;
    public CountDownLatch latch;

    public PartitionController(Identity partitionIdentity,
                               int heartbeatTimeout, int heartbeatInterval,
                               SocketOptions socketOptions,
                               ExecutorService dispatchExecutor,
                               WireSecurity wireSecurity) throws IOException {
        super(partitionIdentity, heartbeatTimeout, heartbeatInterval,
              socketOptions, dispatchExecutor, wireSecurity);
    }

    @Override
    protected NodeData createNode(Heartbeat hb) {
        ControlNode node = new ControlNode(hb, this);
        node.cardinality = cardinality;
        node.latch = latch;
        return node;
    }

}