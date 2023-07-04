/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * An amalgamation of a Kubernetes client, a Kafka Admin client, and a Kafka Agent client.
 */
interface RollClient {
    /** @return true if the pod for this node is not ready according to kubernetes */
    boolean isNotReady(Integer nodeId);

    /** @return The broker state, according to the Kafka Agent */
    int getBrokerState(Integer nodeId);

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param serverId The server id
     * @return The state
     */
    public default State observe(int serverId) {
        if (isNotReady(serverId)) {
            return State.NOT_READY;
        } else {
            try {
                int bs = getBrokerState(serverId);
                if (bs < 3) {
                    return State.RECOVERING;
                } else if (bs == 3) {
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
     * @param podName The name of the pod.
     */
    public void deletePod(String podName);

    /**
     * @return All the topics in the cluster, including internal topics.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    Collection<TopicListing> listTopics() throws ExecutionException, InterruptedException;

    /**
     * Describe the topics with the given ids.
     * If the given {@code topicIds} is large multiple requests (to different brokers) may be used.
     * @param topicIds The topic ids.
     * @return The topic descriptions.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    Stream<TopicDescription> describeTopics(List<Uuid> topicIds) throws InterruptedException, ExecutionException;

    /**
     * Get the {@code min.insync.replicas} of each of the topics in the given {@code topicNames} list.
     * @param topicNames The names of the topics to get the {@code min.insync.replicas} of.
     * @return A map from topic name to its {@code min.insync.replicas}.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    Map<String, Integer> describeTopicMinIsrs(List<String> topicNames) throws InterruptedException, ExecutionException;

    /**
     * @return The id of the server that is the current controller of the cluster.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    int activeController() throws InterruptedException, ExecutionException;


    /**
     * Reconfigure the given server with the given configs
     * @param serverId The server id
     */
    void reconfigureServer(int serverId);

    /**
     * Try to elect the given server as the leader for all the replicas on the server where it's not already
     * the preferred leader.
     * @param serverId The server id
     * @return The number of replicas on the server which it is not leading, but is preferred leader
     */
    int tryElectAllPreferredLeaders(int serverId);

    /**
     * Return the broker configs and broker logger configs for each of the given brokers
     * @param toList The brokers to get the configs for
     * @return A map from broker id to configs
     */
    Map<Integer, Configs> describeBrokerConfigs(List<Integer> toList);

    static record Configs(Config brokerConfigs, Config brokerLoggerConfigs) { }
}
