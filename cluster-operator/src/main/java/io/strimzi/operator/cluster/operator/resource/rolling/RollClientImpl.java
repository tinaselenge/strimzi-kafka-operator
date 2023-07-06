/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

class RollClientImpl implements RollClient {

    private final static int ADMIN_BATCH_SIZE = 200;

    /** Return a future that completes when all of the given futures complete */
    @SuppressWarnings("rawtypes")
    private static CompletableFuture<Void> allOf(List<? extends CompletableFuture<?>> futures) {
        CompletableFuture[] ts = futures.toArray(new CompletableFuture[0]);
        return CompletableFuture.allOf(ts);
    }

    /** Splits the given {@code items} into batches no larger than {@code maxBatchSize}. */
    private static <T> Set<List<T>> batch(List<T> items, int maxBatchSize) {
        Set<List<T>> allBatches = new HashSet<>();
        List<T> currentBatch = null;
        for (var topicId : items) {
            if (currentBatch == null || currentBatch.size() > maxBatchSize) {
                currentBatch = new ArrayList<>();
                allBatches.add(currentBatch);
            }
            currentBatch.add(topicId);
        }
        return allBatches;
    }

    private final Admin admin;

    private final KubernetesClient client;

    private final KafkaAgentClient kafkaAgentClient;

    RollClientImpl(KubernetesClient client, Admin admin, KafkaAgentClient kafkaAgentClient) {
        this.client = client;
        this.admin = admin;
        this.kafkaAgentClient = kafkaAgentClient;
    }

    @Override
    public boolean isNotReady(NodeRef nodeRef) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public BrokerState getBrokerState(NodeRef nodeRef) {
        String podName = null; // TODO convert the nodeId to a podname
        return BrokerState.fromValue((byte) kafkaAgentClient.getBrokerState(podName).code());
    }

    @Override
    public void deletePod(NodeRef nodeRef) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Collection<TopicListing> listTopics() throws ExecutionException, InterruptedException {
        return admin.listTopics(new ListTopicsOptions().listInternal(true)).listings().get();
    }

    @Override
    public List<TopicDescription> describeTopics(List<Uuid> topicIds) throws InterruptedException, ExecutionException {
        var topicIdBatches = batch(topicIds, ADMIN_BATCH_SIZE);
        var futures = new ArrayList<CompletableFuture<Map<Uuid, TopicDescription>>>();
        for (var topicIdBatch : topicIdBatches) {
            var mapKafkaFuture = admin.describeTopics(TopicCollection.ofTopicIds(topicIdBatch)).allTopicIds().toCompletionStage().toCompletableFuture();
            futures.add(mapKafkaFuture);
        }
        allOf(futures).get();
        var topicDescriptions = futures.stream().flatMap(cf -> {
            try {
                return cf.get().values().stream();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        return topicDescriptions.toList();

    }

    @Override
    public int activeController() throws InterruptedException, ExecutionException {
        // TODO when controllers not colocated with brokers, how do we find the active controller?
        DescribeClusterResult dcr = admin.describeCluster();
        var activeController = dcr.controller();
        int controllerId = activeController.get().id();
        return controllerId;
    }

    @Override
    public Map<String, Integer> describeTopicMinIsrs(List<String> topicNames) throws InterruptedException, ExecutionException {
        var topicIdBatches = batch(topicNames, ADMIN_BATCH_SIZE);
        var futures = new ArrayList<CompletableFuture<Map<ConfigResource, Config>>>();
        for (var topicIdBatch : topicIdBatches) {
            var mapKafkaFuture = admin.describeConfigs(topicIdBatch.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toSet())).all().toCompletionStage().toCompletableFuture();
            futures.add(mapKafkaFuture);
        }
        allOf(futures).get();
        var topicDescriptions = futures.stream().flatMap(cf -> {
            try {
                return cf.get().entrySet().stream();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        return topicDescriptions.collect(Collectors.toMap(
                entry -> entry.getKey().name(),
                entry -> Integer.parseInt(entry.getValue().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value())));
    }

    @Override
    public void reconfigureServer(NodeRef nodeRef, KafkaBrokerConfigurationDiff kafkaBrokerConfigurationDiff, KafkaBrokerLoggingConfigurationDiff kafkaBrokerLoggingConfigurationDiff) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int tryElectAllPreferredLeaders(NodeRef nodeRef) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList) {
        return null;
    }

}
