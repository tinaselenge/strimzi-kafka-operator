/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

/**
 * Determines whether the given broker can be rolled without affecting
 * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
 */
class KafkaAvailability2 {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAvailability2.class.getName());

    private final KafkaRollerClient rollerClient;

    private final Reconciliation reconciliation;

    List<TopicDescription> topicDescriptions;

    KafkaAvailability2(Reconciliation reconciliation, KafkaRollerClient rollerClient) {
        this.rollerClient = rollerClient;
        this.reconciliation = reconciliation;
    }

    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    boolean canRoll(int podId) throws InterruptedException {
        LOGGER.debugCr(reconciliation, "Determining whether broker {} can be rolled", podId);
        if (topicDescriptions == null || topicDescriptions.isEmpty()) {
            // 1. Get all topic names
            var topicIds = rollerClient.listTopics().stream().map(TopicListing::topicId).toList();
            LOGGER.debugCr(reconciliation, "Got {} topic ids", topicIds.size());
            LOGGER.traceCr(reconciliation, "Topic ids {}", topicIds);
            topicDescriptions = rollerClient.describeTopics(topicIds);
        }

        Set<TopicDescription> topicsOnGivenBroker = groupTopicsByBroker(topicDescriptions, podId);
        Map<String, Config> topicConfigsOnGivenBroker = rollerClient.describeTopicConfigs(topicsOnGivenBroker.stream().map(TopicDescription::name).toList());

        // 5. join
        boolean canRoll = topicsOnGivenBroker.stream().noneMatch(
                td -> wouldAffectAvailability(podId, topicConfigsOnGivenBroker, td));
        if (!canRoll) {
            LOGGER.debugCr(reconciliation, "Restart pod {} would remove it from ISR, stalling producers with acks=all", podId);
        }
        return canRoll;
    }

    /**
     * Gets topic descriptions
     */
    Collection<TopicDescription> getTopicDescriptions() {
        return topicDescriptions;
    }

    private boolean wouldAffectAvailability(int broker, Map<String, Config> nameToConfig, TopicDescription td) {
        var config = nameToConfig.get(td.name());
        var minIsrConfig = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        int minIsr;
        if (minIsrConfig != null && minIsrConfig.value() != null) {
            minIsr = parseInt(minIsrConfig.value());
            LOGGER.debugCr(reconciliation, "{} has {}={}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
        } else {
            minIsr = -1;
            LOGGER.debugCr(reconciliation, "{} lacks {}.", td.name(), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        }

        for (TopicPartitionInfo pi : td.partitions()) {
            List<Node> isr = pi.isr();
            if (minIsr >= 0) {
                if (pi.replicas().size() <= minIsr) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                                td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                pi.replicas().size());
                    }
                } else if (isr.size() < minIsr
                        && contains(pi.replicas(), broker)) {
                    if (LOGGER.isInfoEnabled()) {
                        String msg;
                        if (contains(isr, broker)) {
                            msg = "{}/{} is already under-replicated (ISR={{}}, replicas=[{}], {}={}); broker {} is in the ISR, " +
                                    "so should not be restarted right now (it would impact consumers).";
                        } else {
                            msg = "{}/{} is already under-replicated (ISR={{}}, replicas=[{}], {}={}); broker {} has a replica, " +
                                    "so should not be restarted right now (it might be first to catch up).";
                        }
                        LOGGER.infoCr(reconciliation, msg,
                                td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                    }
                    return true;
                } else if (isr.size() == minIsr
                        && contains(isr, broker)) {
                    if (minIsr < pi.replicas().size()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.infoCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted.",
                                    td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker);
                        }
                        return true;
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debugCr(reconciliation, "{}/{} will be under-replicated (ISR={{}}, replicas=[{}], {}={}) if broker {} is restarted, but there are only {} replicas.",
                                    td.name(), pi.partition(), nodeList(isr), nodeList(pi.replicas()), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr, broker,
                                    pi.replicas().size());
                        }
                    }
                }
            }
        }
        return false;
    }

    private String nodeList(List<Node> isr) {
        return isr.stream().map(Node::idString).collect(Collectors.joining(","));
    }

    private boolean contains(List<Node> isr, int broker) {
        return isr.stream().anyMatch(node -> node.id() == broker);
    }

    private Set<TopicDescription> groupTopicsByBroker(Collection<TopicDescription> tds, int podId) {
        Set<TopicDescription> topicPartitionInfos = new HashSet<>();
        for (TopicDescription td : tds) {
            LOGGER.traceCr(reconciliation, td);
            for (TopicPartitionInfo pd : td.partitions()) {
                for (Node broker : pd.replicas()) {
                    if (podId == broker.id()) {
                        topicPartitionInfos.add(td);
                    }
                }
            }
        }
        return topicPartitionInfos;
    }
}
