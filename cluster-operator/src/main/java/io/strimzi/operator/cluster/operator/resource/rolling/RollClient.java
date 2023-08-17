/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * An amalgamation of a Kubernetes client, a Kafka Admin client, and a Kafka Agent client.
 */
interface RollClient {
    /** @return true if the pod for this node is not ready according to kubernetes */
    boolean isNotReady(NodeRef nodeRef);

    /** @return The broker state, according to the Kafka Agent */
    BrokerState getBrokerState(NodeRef nodeRef);

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    public static State observe(RollClient client, NodeRef nodeRef) {
        if (client.isNotReady(nodeRef)) {
            return State.NOT_READY;
        } else {
            try {
                var bs = client.getBrokerState(nodeRef);
                if (bs.value() < BrokerState.RUNNING.value()) {
                    return State.RECOVERING;
                } else if (bs.value() == BrokerState.RUNNING.value()) {
                    return State.SERVING;
                } else {
                    return State.NOT_READY;
                }
            } catch (Exception e) {
                return State.NOT_READY;
            }
        }
    }

    /**
     * Delete the pod with the given name, thus causing the restart of the corresponding Kafka server.
     * @param nodeRef The node.
     */
    public void deletePod(NodeRef nodeRef);

    /**
     * @return All the topics in the cluster, including internal topics.
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Collection<TopicListing> listTopics();

    /**
     * Describe the topics with the given ids.
     * If the given {@code topicIds} is large multiple requests (to different brokers) may be used.
     * @param topicIds The topic ids.
     * @return The topic descriptions.
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    List<TopicDescription> describeTopics(List<Uuid> topicIds);

    /**
     * Get the {@code min.insync.replicas} of each of the topics in the given {@code topicNames} list.
     * @param topicNames The names of the topics to get the {@code min.insync.replicas} of.
     * @return A map from topic name to its {@code min.insync.replicas}.
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Map<String, Integer> describeTopicMinIsrs(List<String> topicNames);

    /**
     * @return The id of the server that is the current controller of the cluster.
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    int activeController();


    /**
     * Reconfigure the given server with the given configs
     *
     * @param nodeRef The node
     * @param kafkaBrokerConfigurationDiff The broker config diff
     * @param kafkaBrokerLoggingConfigurationDiff The broker logging diff
     */
    void reconfigureNode(NodeRef nodeRef, KafkaBrokerConfigurationDiff kafkaBrokerConfigurationDiff, KafkaBrokerLoggingConfigurationDiff kafkaBrokerLoggingConfigurationDiff);

    /**
     * Try to elect the given server as the leader for all the replicas on the server where it's not already
     * the preferred leader.
     * @param nodeRef The node
     * @return The number of replicas on the server which it is not leading, but is preferred leader
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    int tryElectAllPreferredLeaders(NodeRef nodeRef);

    /**
     * Return the broker configs and broker logger configs for each of the given brokers
     * @param toList The brokers to get the configs for
     * @return A map from broker id to configs
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList);

    static record Configs(Config brokerConfigs, Config brokerLoggerConfigs) { }
}
