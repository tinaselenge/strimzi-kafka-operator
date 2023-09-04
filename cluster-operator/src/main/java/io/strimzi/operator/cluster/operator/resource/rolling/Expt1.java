/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Expt1 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

//            var qi = admin.describeMetadataQuorum().quorumInfo().get();
//            System.out.printf("admin.describeMetadataQuorum().quorumInfo().get().leaderId() = %s%n", qi.leaderId());
//            System.out.printf("admin.describeMetadataQuorum().quorumInfo().get().leaderEpoch() = %s%n", qi.leaderEpoch());
//            System.out.printf("admin.describeMetadataQuorum().quorumInfo().get().voters() = %s%n", qi.voters());
//            System.out.printf("admin.describeMetadataQuorum().quorumInfo().get().observers() = %s%n", qi.observers());


            DescribeClusterResult describeClusterResult = admin.describeCluster();
            System.out.printf("admin.describeCluster().clusterId().get() = %s%n", describeClusterResult.clusterId().get());
            System.out.printf("admin.describeCluster().nodes().get() = %s%n", describeClusterResult.nodes().get());
            System.out.printf("admin.describeCluster().controller().get() = %s%n", describeClusterResult.controller().get());
        }
    }
}
