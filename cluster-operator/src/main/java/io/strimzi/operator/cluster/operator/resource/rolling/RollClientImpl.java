/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import io.strimzi.operator.common.operator.resource.PodOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

class RollClientImpl implements RollClient, PlatformClient {

    private final static int ADMIN_BATCH_SIZE = 200;
    private final String namespace;
    private final Reconciliation reconciliation;

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

    private final PodOperator podOps;

    private final KafkaAgentClient kafkaAgentClient;

    RollClientImpl(Reconciliation reconciliation,
                   PodOperator podOps,
                   String namespace,
                   Admin admin, KafkaAgentClient kafkaAgentClient) {
        this.reconciliation = reconciliation;
        this.podOps = podOps;
        this.namespace = namespace;
        this.admin = admin;
        this.kafkaAgentClient = kafkaAgentClient;
    }

    @Override
    public boolean isNotReady(NodeRef nodeRef) {
        return !podOps.isReady(namespace, nodeRef.podName());
    }

    @Override
    public BrokerState getBrokerState(NodeRef nodeRef) {
        String podName = nodeRef.podName();
        return BrokerState.fromValue((byte) kafkaAgentClient.getBrokerState(podName).code());
    }

    @Override
    public void restartNode(NodeRef nodeRef) {
        var pod = podOps.get(namespace, nodeRef.podName());
        podOps.restart(reconciliation, pod, 60_000);
    }

    @Override
    public Collection<TopicListing> listTopics() {
        try {
            return admin.listTopics(new ListTopicsOptions().listInternal(true)).listings().get();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public List<TopicDescription> describeTopics(List<Uuid> topicIds) {
        try {
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
                    throw new UncheckedInterruptedException(e);
                } catch (ExecutionException e) {
                    throw new UncheckedExecutionException(e);
                }
            });
            return topicDescriptions.toList();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public int activeController() {
        try {
            // TODO when controllers not colocated with brokers, how do we find the active controller?
            DescribeClusterResult dcr = admin.describeCluster();
            var activeController = dcr.controller().get();
            return activeController.id();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Map<String, Integer> describeTopicMinIsrs(List<String> topicNames) {
        try {
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
                    throw new UncheckedInterruptedException(e);
                } catch (ExecutionException e) {
                    throw new UncheckedExecutionException(e);
                }
            });
            return topicDescriptions.collect(Collectors.toMap(
                    entry -> entry.getKey().name(),
                    entry -> Integer.parseInt(entry.getValue().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value())));
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public void reconfigureNode(NodeRef nodeRef, KafkaBrokerConfigurationDiff kafkaBrokerConfigurationDiff, KafkaBrokerLoggingConfigurationDiff kafkaBrokerLoggingConfigurationDiff) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int tryElectAllPreferredLeaders(NodeRef nodeRef) {
        try {
            // find all partitions where the node is the preferred leader
            // we could do listTopics then describe all the topics, but that would scale poorly with number of topics
            // using describe log dirs should be more efficient
            var topicsOnNode = admin.describeLogDirs(List.of(nodeRef.nodeId())).allDescriptions().get()
                    .get(nodeRef).values().stream()
                    .flatMap(x -> x.replicaInfos().keySet().stream())
                    .map(TopicPartition::topic)
                    .collect(Collectors.toSet());

            var topicDescriptionsOnNode = admin.describeTopics(topicsOnNode).allTopicNames().get().values();
            var toElect = new HashSet<TopicPartition>();
            for (TopicDescription td : topicDescriptionsOnNode) {
                for (TopicPartitionInfo topicPartitionInfo : td.partitions()) {
                    if (!topicPartitionInfo.replicas().isEmpty()
                            && topicPartitionInfo.replicas().get(0).id() == nodeRef.nodeId() // this node is preferred leader
                            && topicPartitionInfo.leader().id() != nodeRef.nodeId()) { // this onde is not current leader
                        toElect.add(new TopicPartition(td.name(), topicPartitionInfo.partition()));
                    }
                }
            }

            var electionResults = admin.electLeaders(ElectionType.PREFERRED, toElect).partitions().get();

            long count = electionResults.values().stream()
                    .filter(Optional::isPresent)
                    .count();
            return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList) {
        try {
            var dc = admin.describeConfigs(toList.stream().map(nodeRef -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeRef.nodeId()))).toList());
            var result = dc.all().get();

            return toList.stream().collect(Collectors.toMap(NodeRef::nodeId,
                    nodeRef -> new Configs(result.get(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeRef))),
                            result.get(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, String.valueOf(nodeRef)))
                            )));
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

}
