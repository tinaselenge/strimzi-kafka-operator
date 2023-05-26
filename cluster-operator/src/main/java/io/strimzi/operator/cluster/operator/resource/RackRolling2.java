/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RackRolling2 {

    record Replica(String topicName, int partitionId, int broker, boolean inSync) { }

    record Cluster(List<Replica> replicas) {

        /** The set of brokers in the cluster */
        Set<Integer> brokers() {
            return null;
        }

        /** All the partitions in the cluster */
        Set<TopicPartition> partitions() {
            return null;
        }

        /** The replicas on the given broker */
        List<Replica> brokerReplicas(int brokerId) {
            return null;
        }

        /** The replic of the given partition on the given broker */
        Replica replica(int broker, String topic, int partition) {
            return null;
        }

        /**
         * The partitions that both brokers have replicas of
         */
        Set<TopicPartition> commonPartitions(int brokerId1, int brokerId2) {
            return null;
        }

        /** The brokers that replicate the given partition */
        Set<Integer> replicatingBrokers(String topicName, int partitionId) {
            return null;
        }
    }

    public static Set<Integer> rollable(Cluster cluster, Map<String, Integer> minIrs) {
        for (int broker1 : cluster.brokers()) {
            // TODO check broker1 is safe to restart
            for (int broker2 : cluster.brokers()) {
                // TODO check broker2 is safe to restart

                // Can we roll them together?
                if (broker1 < broker2) {
                    var commonPartitions = cluster.commonPartitions(broker1, broker2);
                    boolean compatible = true;
                    for (var cp : commonPartitions) {
                        var r1 = cluster.replica(broker1, cp.topic(), cp.partition());
                        var r2 = cluster.replica(broker2, cp.topic(), cp.partition());
                        if (false) { // TODO
                            compatible = false;
                            break;
                        }
                    }
                    if (compatible) {
                        // brokers can be rolled together
                    }
                }
            }
        }
        throw new RuntimeException("TODO"); // TODO
    }
}
