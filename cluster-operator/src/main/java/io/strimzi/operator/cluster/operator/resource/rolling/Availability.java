/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
public class Availability {

    protected static Map<Integer, KafkaNode> nodeIdToKafkaNode(RollClient rollClient, Map<Integer, Context> contextMap, Map<Integer, NodeRoles> nodesNeedingRestart) {

        Map<Integer, KafkaNode> nodeIdToKafkaNode = new HashMap<>();

        // Get all the topics in the cluster
        Collection<TopicListing> topicListings = rollClient.listTopics();

        // batch the describeTopics requests to avoid trying to get the state of all topics in the cluster
        var topicIds = topicListings.stream().map(TopicListing::topicId).toList();

        // Convert the TopicDescriptions to the Server and Replicas model
        List<TopicDescription> topicDescriptions = rollClient.describeTopics(topicIds);

        topicDescriptions.forEach(topicDescription -> {
            topicDescription.partitions().forEach(partition -> {
                partition.replicas().forEach(replicatingBroker -> {
                    var kafkaNode = nodeIdToKafkaNode.computeIfAbsent(replicatingBroker.id(),
                            ig -> {
                                NodeRoles nodeRoles = contextMap.get(replicatingBroker.id()).currentRoles();
                                return new KafkaNode(replicatingBroker.id(), nodeRoles.controller(), nodeRoles.broker(), new HashSet<>());
                            });
                    kafkaNode.replicas().add(new Replica(
                            replicatingBroker,
                            topicDescription.name(),
                            partition.partition(),
                            partition.isr()));
                });
            });
        });

        // Add any servers which we know about but which were absent from any partition metadata
        // i.e. brokers without any assigned partitions
        nodesNeedingRestart.forEach((nodeId, nodeRoles) -> nodeIdToKafkaNode.putIfAbsent(nodeId, new KafkaNode(nodeId, nodeRoles.controller(), nodeRoles.broker(), Set.of())));

        return nodeIdToKafkaNode;
    }

    protected static boolean anyReplicaWouldBeUnderReplicated(KafkaNode kafkaNode,
                                                              Map<String, Integer> minIsrByTopic) {
        for (var replica : kafkaNode.replicas()) {
            var topicName = replica.topicName();
            Integer minIsr = minIsrByTopic.get(topicName);
            if (wouldBeUnderReplicated(minIsr, replica)) {
                return true;
            }
        }
        return false;
    }


    private static boolean wouldBeUnderReplicated(Integer minIsr, Replica replica) {
        final boolean wouldByUnderReplicated;
        if (minIsr == null) {
            // if topic doesn't have minISR then it's fine
            wouldByUnderReplicated = false;
        } else {
            // else topic has minISR
            // compute spare = size(ISR) - minISR
            int sizeIsr = replica.isrSize();
            int spare = sizeIsr - minIsr;
            if (spare > 0) {
                // if (spare > 0) then we can restart the broker hosting this replica
                // without the topic being under-replicated
                wouldByUnderReplicated = false;
            } else if (spare == 0) {
                // if we restart this broker this replica would be under-replicated if it's currently in the ISR
                // if it's not in the ISR then restarting the server won't make a difference
                wouldByUnderReplicated = replica.isInIsr();
            } else {
                // this partition is already under-replicated
                // if it's not in the ISR then restarting the server won't make a difference
                // but in this case since it's already under-replicated let's
                // not possible prolong the time to this server rejoining the ISR
                wouldByUnderReplicated = true;
            }
        }
        return wouldByUnderReplicated;
    }
}
