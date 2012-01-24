package com.salesforce.ouroboros.testUtils;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import org.smartfrog.services.anubis.partition.test.controller.Controller;
import org.smartfrog.services.anubis.partition.test.controller.NodeData;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.smartfrog.services.anubis.partition.wire.msg.Heartbeat;
import org.smartfrog.services.anubis.partition.wire.security.WireSecurity;

import com.hellblazer.pinkie.SocketOptions;

public class PartitionController extends Controller {
    public int            cardinality;
    public CountDownLatch latch;

    public PartitionController(Timer timer, long checkPeriod, long expirePeriod,
                        Identity partitionIdentity, long heartbeatTimeout,
                        long heartbeatInterval, SocketOptions socketOptions,
                        Executor dispatchExecutor, WireSecurity wireSecurity)
                                                                             throws IOException {
        super(timer, checkPeriod, expirePeriod, partitionIdentity,
              heartbeatTimeout, heartbeatInterval, socketOptions,
              dispatchExecutor, wireSecurity);
    }

    @Override
    protected NodeData createNode(Heartbeat hb) {
        ControlNode node = new ControlNode(hb, this);
        node.cardinality = cardinality;
        node.latch = latch;
        return node;
    }

}