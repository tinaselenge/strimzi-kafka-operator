/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.NodeRef;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An abstraction over a Kafka Admin client.
 */
public interface KafkaRollerClient {
    /**
     * Sets admin client for brokers.
     * @param brokerNodes to bootstrap
     **/
    void initialiseBrokerAdmin(Set<NodeRef> brokerNodes);

    /**
     * Sets admin client for controllers.
     * @param controllerNodes to bootstrap
     **/
    void initialiseControllerAdmin(Set<NodeRef> controllerNodes);

    /**
     * Closes controller admin client
     **/
    void closeControllerAdminClient();

    /**
     * Closes broker admin client
     **/
    void closeBrokerAdminClient();

    /**
     * Checks if node is responsive by connecting to it via Admin API
     *
     * @param nodeRef The node ref
     * @param isBroker a boolean value informing if it's a broker node
     * @return true if node is responsive, otherwise false
     */
    boolean canConnectToNode(NodeRef nodeRef, boolean isBroker);

    /**
     * List topics
     *
     * @return All the topics in the cluster, including internal topics.
     */
    Collection<TopicListing> listTopics() throws InterruptedException;

    /**
     * Describe the topics with the given ids.
     * If the given {@code topicIds} is large, multiple requests (to different brokers) may be used.
     *
     * @param topicIds The topic ids.
     * @return The topic descriptions.
     */
    List<TopicDescription> describeTopics(List<Uuid> topicIds) throws InterruptedException;


    /**
     * Get the configs of each of the topics in the given {@code topicNames} list.
     * @param topicNames The names of the topics to get the configs for.
     * @return A map from topic name to its {@code Config}.
     */
    Map<String, Config> describeTopicConfigs(List<String> topicNames) throws InterruptedException;

    /**
     * Describe the metadata quorum info.
     *
     * @return {@code QuorumInfo}.
     */
    QuorumInfo describeMetadataQuorum() throws InterruptedException;

    /**
     * Describe the metadata quorum and return the active controller's id.
     *
     * @return active controller's id.
     */
    int getActiveControllerId();

    /**
     * Reconfigure the given server with the given configs
     *
     * @param nodeRef                The node
     * @param kafkaConfigurationDiff The broker config diff
     * @param isBroker               a boolean value informing if it's a broker node
     */
    void reconfigureNode(NodeRef nodeRef, KafkaConfigurationDiff kafkaConfigurationDiff, boolean isBroker) throws InterruptedException;

    /**
     * Try to elect the given server as the leader for all the replicas on the server where it's not already
     * the preferred leader.
     * @param nodeRef The node
     * @return The number of replicas on the server which it is not leading, but is preferred leader
     */
    int tryElectAllPreferredLeaders(NodeRef nodeRef) throws InterruptedException;

    /**
     * Return the Kafka broker configs and logger configs for each of the given nodes
     *
     * @param nodeId The nodes to get the configs for
     * @return A map from node id to configs
     */
    Config describeBrokerConfigs(int nodeId) throws InterruptedException;

    /**
     * Return the Kafka controller configs for each of the given nodes
     *
     * @param nodeId The nodes to get the configs for
     * @return A map from node id to configs
     */
    Config describeControllerConfigs(int nodeId) throws InterruptedException;
}