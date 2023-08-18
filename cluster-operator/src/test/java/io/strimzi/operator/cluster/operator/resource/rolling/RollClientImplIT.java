/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KRaftCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@ExtendWith(KafkaClusterExtension.class)
class RollClientImplIT {

    @KRaftCluster(numControllers = 3)
    KafkaCluster cluster;

    //@Test
    public void shouldWork() throws ExecutionException, InterruptedException {
        // Create some topics with partitions on known brokers
        var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        System.out.printf("Nodes %s%n", describeClusterResult.nodes().get());
        System.out.printf("Controller %s%n", describeClusterResult.controller().get());
    }

    // TODO test each of the methods

}