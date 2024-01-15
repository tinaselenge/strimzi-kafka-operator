/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
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

    /**
     * Sets admin client for brokers.
     **/
    void initialiseBrokerAdmin();

    /**
     * Sets admin client for controllers.
     **/
    void initialiseControllerAdmin();

    boolean cannotConnectToNode(NodeRef nodeRef, boolean controller);

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
     * If the given {@code topicIds} is large, multiple requests (to different brokers) may be used.
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
     * Describe the metadata quorum info.
     * @return A map from controller quorum followers to their {@code lastCaughtUpTimestamps}.
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Map<Integer, Long> quorumLastCaughtUpTimestamps();

    /**
     * @return The id of the node that is the active controller of the cluster.
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
     * Return the Kafka broker configs and logger configs for each of the given nodes
     * @param toList The nodes to get the configs for
     * @return A map from node id to configs
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList);

    /**
     * Return the Kafka controller configs for each of the given nodes
     * @param toList The nodes to get the configs for
     * @return A map from node id to configs
     * @throws io.strimzi.operator.common.UncheckedExecutionException
     * @throws io.strimzi.operator.common.UncheckedInterruptedException
     */
    Map<Integer, Configs> describeControllerConfigs(List<NodeRef> toList);
}
