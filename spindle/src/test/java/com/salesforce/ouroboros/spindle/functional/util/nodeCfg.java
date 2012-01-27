package com.salesforce.ouroboros.spindle.functional.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.smartfrog.services.anubis.partition.Partition;
import org.smartfrog.services.anubis.partition.util.Identity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.uuid.Generators;
import com.salesforce.ouroboros.Node;
import com.salesforce.ouroboros.partition.Switchboard;
import com.salesforce.ouroboros.spindle.Coordinator;
import com.salesforce.ouroboros.spindle.Weaver;
import com.salesforce.ouroboros.spindle.WeaverConfigation;

@Configuration
public class nodeCfg {
    public static final int MAX_SEGMENT_SIZE = 1024 * 1024;

    public final File       directory;
    @Autowired
    private Partition       partitionManager;
    @Autowired
    private Identity        partitionIdentity;

    public nodeCfg() {
        try {
            directory = File.createTempFile("CoordinatorIntegration-", ".root");
        } catch (IOException e) {
            throw new IllegalStateException();
        }
        directory.delete();
        directory.mkdirs();
        directory.deleteOnExit();
    }

    @Bean
    public Coordinator coordinator() throws IOException {
        return new Coordinator(timer(), switchboard(), weaver());
    }

    @Bean
    public Node memberNode() {
        return new Node(partitionIdentity.id, partitionIdentity.id,
                        partitionIdentity.id);
    }

    @Bean
    public Switchboard switchboard() {
        Switchboard switchboard = new Switchboard(
                                                  memberNode(),
                                                  partitionManager,
                                                  Generators.timeBasedGenerator());
        return switchboard;
    }

    @Bean
    public ScheduledExecutorService timer() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    public Weaver weaver() throws IOException {
        return new Weaver(weaverConfiguration());
    }

    private WeaverConfigation weaverConfiguration() throws IOException {
        WeaverConfigation weaverConfigation = new WeaverConfigation();
        weaverConfigation.setId(memberNode());
        weaverConfigation.addRoot(directory);
        weaverConfigation.setMaxSegmentSize(maxSegmentSize());
        return weaverConfigation;
    }

    protected long maxSegmentSize() {
        return MAX_SEGMENT_SIZE;
    }
}