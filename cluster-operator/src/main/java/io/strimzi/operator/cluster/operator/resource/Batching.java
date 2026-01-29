/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Deal with the batching mechanism for the new Kafka roller
 */
public class Batching {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Batching.class);

    private final Reconciliation reconciliation;
    private final Collection<TopicDescription> topicDescriptions;

    /**
     * Batching constructor
     *
     * @param reconciliation Reconciliation
     * @param topicDescriptions topicDescription
     */
    public Batching(Reconciliation reconciliation, Collection<TopicDescription> topicDescriptions) {
        this.reconciliation = reconciliation;
        this.topicDescriptions = topicDescriptions;
    }

    /**
     * Partition the given {@code brokers}
     * into cells that can be rolled in parallel because they
     * contain no replicas in common.
     */
    private List<Set<KafkaNode>> cells(Set<Integer> brokers) {

        // find brokers that are individually rollable
        var rollable = brokers.stream()
                .map(nodeId -> new KafkaNode(nodeId, getReplicasForNode(nodeId)))
                .collect(Collectors.toSet());

        // partition the set under the equivalence relation "shares a partition with"
        Set<Set<KafkaNode>> disjoint = partitionByHasAnyReplicasInCommon(rollable);
        // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
        // We find the biggest set of brokers which can parallel-rolled
        return disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
    }

    private Set<Set<KafkaNode>> partitionByHasAnyReplicasInCommon(Set<KafkaNode> rollable) {
        Set<Set<KafkaNode>> disjoint = new HashSet<>();
        for (var node : rollable) {
            var nodeReplicas = node.replicas();
            Set<Set<KafkaNode>> merge = new HashSet<>();
            for (Set<KafkaNode> cell : disjoint) {
                if (!containsAny(reconciliation, node, nodeReplicas, cell)) {
                    LOGGER.debugCr(reconciliation, "Add {} to {{}}", node.id(), idsOf(cell));
                    merge.add(cell);
                    merge.add(Set.of(node));
                    // problem is here, we're iterating over all cells (ones which we've decided should be disjoint)
                    // and we merged them in violation of that
                    // we could break here at the end of the if block (which would be correct)
                    // but it might not be optimal (in the sense of forming large cells)
                    break;
                }
            }
            if (merge.isEmpty()) {
                LOGGER.debugCr(reconciliation, "New cell: {{}}", node.id());
                disjoint.add(Set.of(node));
            } else {
                LOGGER.debugCr(reconciliation, "Merge {}", idsOf2(merge));
                for (Set<KafkaNode> r : merge) {
                    LOGGER.debugCr(reconciliation, "Remove cell: {}", idsOf(r));
                    disjoint.remove(r);
                }
                Set<KafkaNode> newCell = union(merge);
                LOGGER.debugCr(reconciliation, "New cell: {{}}", idsOf(newCell));
                disjoint.add(newCell);
            }
            LOGGER.debugCr(reconciliation, "Disjoint cells now: {}", idsOf2(disjoint));
        }
        return disjoint;
    }

    /**
     * Split the given cells into batches,
     * taking account of {@code acks=all} availability and the given maxBatchSize
     *
     * @param nodesToBatch Nodes that need to be split into batches
     * @param maxBatchSize Maximum size of a batch
     *
     * @return Set of broker ids that are in the same batch so they can be restarted in parallel
     */
    Set<Integer> getBatchedBrokersToRestart(Set<Integer> nodesToBatch, int maxBatchSize) {
        // Split the set of all brokers into subsets of brokers that can be rolled in parallel
        var cells = cells(nodesToBatch);
        int cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Cell {}: {}", ++cellNum, cell);
        }

        cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Restart-eligible cell {}: {}", ++cellNum, cell);
        }

        List<Set<Integer>> batches = new ArrayList<>();
        for (var cell : cells) {
            List<Set<Integer>> availBatches = new ArrayList<>();
            for (var kafkaNode : cell) {
                LOGGER.debugCr(reconciliation, "No replicas of node {} will be unavailable => add to batch",
                        kafkaNode.id());
                var currentBatch = availBatches.isEmpty() ? null : availBatches.getLast();
                if (currentBatch == null || currentBatch.size() >= maxBatchSize) {
                    currentBatch = new HashSet<>();
                    availBatches.add(currentBatch);
                }
                currentBatch.add(kafkaNode.id());
            }
            batches.addAll(availBatches);
        }

        LOGGER.debugCr(reconciliation, "Batches {}", Batching.nodeIdsToString2(batches));
        return pickBestBatchForRestart(batches);
    }

    static <T> T elementInIntersection(Set<T> set, Set<T> set2) {
        for (T t : set) {
            if (set2.contains(t)) {
                return t;
            }
        }
        return null;
    }

    static boolean containsAny(Reconciliation reconciliation,
                               KafkaNode node,
                               Set<Replica> nodeReplicas,
                               Set<KafkaNode> cell) {
        for (var b : cell) {
            var commonReplica = elementInIntersection(b.replicas(), nodeReplicas);
            if (commonReplica != null) {
                LOGGER.debugCr(reconciliation, "Nodes {} and {} have at least {} in common",
                        node.id(), b.id(), commonReplica);
                return true;
            }
        }
        LOGGER.debugCr(reconciliation, "Node {} has no replicas in common with any of the nodes in {}",
                node.id(), idsOf(cell));
        return false;
    }

    private static String idsOf(Collection<KafkaNode> cell) {
        return cell.stream()
                .map(kafkaNode -> Integer.toString(kafkaNode.id()))
                .collect(Collectors.joining(",", "{", "}"));
    }

    private static String idsOf2(Collection<? extends Collection<KafkaNode>> merge) {
        return merge.stream()
                .map(Batching::idsOf)
                .collect(Collectors.joining(",", "{", "}"));
    }

    /** Returns a new set that is the union of each of the sets in the given {@code merge}. I.e. flatten without duplicates. */
    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }


    private static String nodeIdsToString2(Collection<? extends Collection<Integer>> merge) {
        return merge.stream()
                .map(Batching::nodeIdsToString)
                .collect(Collectors.joining(",", "{", "}"));
    }

    private static String nodeIdsToString(Collection<Integer> cell) {
        return cell.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "{", "}"));
    }

    /**
     * Pick the "best" batch to be restarted.
     * This is the largest batch of available servers
     * @return the "best" batch to be restarted
     */
    private Set<Integer> pickBestBatchForRestart(List<Set<Integer>> batches) {
        if (batches.isEmpty()) {
            return Set.of();
        }

        // if none of the batches contain more than 1 node, return a set with unready node if there is any, otherwise the first set.
        if (batches.stream().noneMatch(set -> set.size() > 1)) {
            return batches.getFirst();
        }

        var bestBatch = batches.stream().sorted(Comparator.comparing(Set::size)).toList().getLast();

        LOGGER.debugCr(reconciliation, "Best batch {}", Batching.nodeIdsToString(bestBatch));
        return bestBatch;
    }


    private Set<Replica> getReplicasForNode(int nodeId) {
        Set<Replica> replicas = new HashSet<>();
        topicDescriptions.forEach(topicDescription -> topicDescription.partitions()
                .forEach(topicPartitionInfo -> topicPartitionInfo.replicas()
                        .forEach(replicatingBroker -> {
                            if (replicatingBroker.id() == nodeId) {
                                replicas.add(new Replica(
                                        replicatingBroker,
                                        topicDescription.name(),
                                        topicPartitionInfo.partition(),
                                        topicPartitionInfo.isr(),
                                        topicPartitionInfo.replicas()
                                ));
                            }
                        })));
        return replicas;
    }

    /**
     * A replica on a particular Kafka Node
     *
     * @param topicName         The name of the topic
     * @param partitionId       The partition id
     * @param isrSize           If the broker hosting this replica is in the ISR for the partition of this replica
     * @param replicasSize      Size of the replicas this topic partition has
     *                    this is the size of the ISR.
     *                    If the broker hosting this replica is NOT in the ISR for the partition of this replica
     *                    this is the negative of the size of the ISR.
     *                    In other words, the magnitude is the size of the ISR and the sign will be negative
     *                    if the broker hosting this replica is not in the ISR.
     */
    private record Replica(String topicName, int partitionId, short isrSize, short replicasSize) {
        private Replica(Node broker, String topicName, int partitionId, Collection<Node> isr, Collection<Node> replicas) {
            this(topicName, partitionId, (short) (isr.contains(broker) ? isr.size() : -isr.size()), (short) replicas.size());
        }

        @Override
        public String toString() {
            return topicName + "-" + partitionId;
        }
    }

    /**
     * Information about a Kafka node (which may be a broker, controller, or both) and its replicas.
     * @param id The id of the server
     * @param replicas The replicas on this server
     */
    private record KafkaNode(int id, Set<Replica> replicas) {
    }
}