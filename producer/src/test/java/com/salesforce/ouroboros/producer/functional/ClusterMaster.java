package com.salesforce.ouroboros.producer.functional;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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
    Semaphore                      semaphore = new Semaphore(0);
    final Switchboard              switchboard;
    final ScheduledExecutorService timer     = Executors.newSingleThreadScheduledExecutor();
    private ServerSocket           socket;

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
        switch (type) {
            case BOOTSTRAP_SPINDLES:
                semaphore.release();
                break;
            default:
                break;

        }
    }

    @Override
    public void dispatch(ChannelMessage type, Node sender,
                         Serializable[] arguments, long time) {
        switch (type) {
            case MIRROR_OPENED:
                semaphore.release();
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

    public void failover() {
        switchboard.ringCast(new Message(switchboard.getId(),
                                         FailoverMessage.FAILOVER));
    }

    public boolean open(UUID channel, long timeout, TimeUnit unit)
                                                                  throws InterruptedException {
        switchboard.ringCast(new Message(switchboard.getId(),
                                         ChannelMessage.OPEN,
                                         new Serializable[] { channel }));
        switchboard.ringCast(new Message(switchboard.getId(),
                                         ChannelMessage.PRIMARY_OPENED,
                                         new Serializable[] { channel }));
        switchboard.ringCast(new Message(switchboard.getId(),
                                         ChannelMessage.MIRROR_OPENED,
                                         new Serializable[] { channel }));
        return acquire(timeout, unit).get();
    }

    public void prepare() {
        switchboard.ringCast(new Message(switchboard.getId(),
                                         FailoverMessage.PREPARE));
    }

    public boolean bootstrapSpindles(Node[] spindles, long timeout,
                                     TimeUnit unit) throws InterruptedException {
        switchboard.ringCast(new Message(switchboard.getId(),
                                         BootstrapMessage.BOOTSTRAP_SPINDLES,
                                         (Serializable) spindles));
        return acquire(timeout, unit).get();
    }

    @Override
    public void stabilized() {
    }

    private AtomicBoolean acquire(long timeout, TimeUnit unit)
                                                              throws InterruptedException {
        final AtomicBoolean acquired = new AtomicBoolean(true);
        timer.schedule(new Runnable() {
            @Override
            public void run() {
                acquired.set(false);
                semaphore.release();
            }
        }, timeout, unit);
        semaphore.acquire();
        return acquired;
    }

    @PostConstruct
    public void openChannel() throws UnknownHostException, IOException {
        socket = new ServerSocket(FakeSpindle.PORT, 100,
                                  InetAddress.getByName("127.0.01"));
    }

    @PreDestroy
    public void closeChannel() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }
}