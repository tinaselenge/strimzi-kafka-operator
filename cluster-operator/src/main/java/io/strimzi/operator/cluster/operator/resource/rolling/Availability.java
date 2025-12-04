/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
//TODO: Make this class easier to debug
//TODO: Add a test class for this
class Availability {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Availability.class.getName());

    private final Reconciliation reconciliation;

    private final List<TopicDescription> topicDescriptions;

    private final Map<String, Integer> minIsrByTopic;

    Availability(Reconciliation reconciliation, RollClient rollClient) {
        this.reconciliation = reconciliation;
        // 1. Get all topics
        Collection<TopicListing> topicListings = rollClient.listTopics();
        // 2. Get topic descriptions
        //TODO: Refactor this out so that it can be used by both Availability and Batching classes independently
        topicDescriptions = rollClient.describeTopics(topicListings.stream().map(TopicListing::topicId).toList());
        // 2. Get topic minISR configurations
        minIsrByTopic = rollClient.describeTopicMinIsrs(rollClient.listTopics().stream().map(TopicListing::name).toList());
    }

    protected boolean anyPartitionWouldBeUnderReplicated(int nodeId) {
        var replicas = getReplicasForNode(nodeId);
        for (var replica : replicas) {
            var topicName = replica.topicName();
            Integer minIsr = minIsrByTopic.get(topicName);
            if (wouldBeUnderReplicated(minIsr, replica, nodeId)) {
                return true;
            }
        }
        return false;
    }

    protected Set<Replica> getReplicasForNode(int nodeId) {
        Set<Replica> replicas = new HashSet<>();
        topicDescriptions.forEach(topicDescription -> {
            topicDescription.partitions().forEach(topicPartitionInfo -> {
                topicPartitionInfo.replicas().forEach(replicatingBroker -> {
                    if (replicatingBroker.id() == nodeId) {
                        replicas.add(new Replica(
                                replicatingBroker,
                                topicDescription.name(),
                                topicPartitionInfo.partition(),
                                topicPartitionInfo.isr(),
                                topicPartitionInfo.replicas()
                        ));
                    }
                });
            });
        });
        return replicas;
    }

    private boolean wouldBeUnderReplicated(Integer minIsr, Replica replica, int nodeId) {
        final boolean wouldByUnderReplicated;
        if (minIsr == null) {
            // if topic doesn't have minISR then it's fine
            LOGGER.debugCr(reconciliation, "{} lacks {}.", replica.topicName(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
            wouldByUnderReplicated = false;
        } else if (replica.size() <= minIsr) {
            LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated if broker {} is restarted but there only {}} replicas and {}={}}",
                    replica.topicName(), replica.partitionId(), nodeId, replica.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
            wouldByUnderReplicated = false;
        } else {
            // compute spare = size(ISR) - minISR
            int sizeIsr = replica.isrSize();
            int spare = sizeIsr - minIsr;
            if (spare > 0) {
                // if (spare > 0) then we can restart the broker hosting this replica
                // without the topic being under-replicated
                wouldByUnderReplicated = false;
                LOGGER.debugCr(reconciliation, "{}/{} will not be under-replicated (ISR size ={{}}, replicas size=[{}], {}={}) if broker {} is restarted.",
                        replica.topicName(), replica.partitionId(), replica.isrSize(), replica.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, nodeId);
            } else if (spare == 0) {
                // if we restart this broker this replica would be under-replicated if it's currently in the ISR
                // if it's not in the ISR then restarting the server won't make a difference
                wouldByUnderReplicated = replica.isInIsr();
                if (wouldByUnderReplicated) {
                    LOGGER.infoCr(reconciliation, "{}/{} is already under-replicated (ISR size ={{}}, replicas size=[{}], {}={}); broker {} is in the ISR, " +
                                    "so should not be restarted right now as it would impact the consumers",
                            replica.topicName(), replica.partitionId(), replica.isrSize(), replica.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, nodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "{}/{} is already under-replicated (ISR size ={{}}, replicas size=[{}], {}={}); broker {} is not in the ISR, " +
                            "so restarting it would not make difference",
                            replica.topicName(), replica.partitionId(), replica.isrSize(), replica.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, nodeId);
                }
            } else {
                // this partition is already under-replicated
                // if it's not in the ISR then restarting the server won't make a difference
                // but in this case since it's already under-replicated let's
                // not possible prolong the time to this server rejoining the ISR
                wouldByUnderReplicated = true;
                LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (ISR size ={{}}, replicas size=[{}], {}={}) if broker {} is restarted.",
                        replica.topicName(), replica.partitionId(), replica.isrSize(), replica.size(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, nodeId);
            }
        }
        return wouldByUnderReplicated;
    }
}
