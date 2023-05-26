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
 * An abstraction over both a Kafka Admin client, and a Kafka Agent client.
 */
interface RollClient {

    /** @return The broker state, according to the Kafka Agent */
    BrokerState getBrokerState(NodeRef nodeRef);

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

    // describeNodes(): List<NodeDescription>
    // record NodeDescription(int nodeId, boolean activeController, boolean controller, boolean broker)
    // TODO or should we use the NodeRef info for controller-ness and broker-ness?
    // The alternative (assuming KIP-919) is to figure out the node type from the configs, which we can get,
    // even for controllers, via the Admin client.
    // TODO think about migration
    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-866+ZooKeeper+to+KRaft+Migration
    // When Preparing the Cluster:
    // * Upgrade the cluster to the bridge version
    // * Necessary configs are defined
    // * All brokers online
    //
    // During Controller Migration:
    // * Controller quorum nodes provisioned with the necessary configs set
    // * Quorum established and leader elected
    // * KRaft leader becomes ZK controller. Leader copies all existing metadata to the log.
    //   During this time metadata updates (from brokers) will be allowed.
    // * So we should avoid doing anything that would require updates,
    //   such as restarting brokers, rebalances, creating deleting topics etc.
    //
    // During Broker Migration:
    // * This is basically just a rolling restart
    // * The active ZK controller will also be the active KRaft controller
    // * We can assume pure controllers
    // * During the broker rolling restart there will be ZK and KRaft brokers in the cluster
    //
    // When Finalizing the Migration:
    // * This is also a rolling restart (+config change), this time of the controllers only.
    //
    // Open questions
    // How do we know what migration state we're in, do we use the metric?

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
