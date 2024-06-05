/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Deal with the batching mechanism for the new Kafka roller
 */
public class Batching {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Batching.class);

    /**
     * Returns a batch of broker nodes that have no topic partitions in common and have no impact on cluster availability if restarted.
     */
    protected static Set<KafkaNode> nextBatchBrokers(Reconciliation reconciliation,
                                                   RollClient rollClient,
                                                   Map<Integer, Context> contextMap,
                                                   Map<Integer, NodeRoles> nodesNeedingRestart,
                                                   int maxRestartBatchSize) {

        Map<Integer, KafkaNode> nodeIdToKafkaNode = Availability.nodeIdToKafkaNode(rollClient, contextMap, nodesNeedingRestart);

        // TODO somewhere in here we need to take account of partition reassignments
        //      e.g. if a partition is being reassigned we expect its ISR to change
        //      (see https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment#KIP455:CreateanAdministrativeAPIforReplicaReassignment-Algorithm
        //      which guarantees that addingReplicas are honoured before removingReplicas)
        //      If there are any removingReplicas our availability calculation won't account for the fact
        //      that the controller may shrink the ISR during the reassignment.

        // Split the set of all brokers into subsets of brokers that can be rolled in parallel
        var cells = cells(reconciliation, nodeIdToKafkaNode.values());
        int cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Cell {}: {}", ++cellNum, cell);
        }

        // filter each cell by brokers that actually need to be restarted
        cells = cells.stream()
                .map(cell -> cell.stream()
                        .filter(kafkaNode -> nodesNeedingRestart.containsKey(kafkaNode.id())).collect(Collectors.toSet()))
                .toList();
        cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Restart-eligible cell {}: {}", ++cellNum, cell);
        }

        var minIsrByTopic = rollClient.describeTopicMinIsrs(rollClient.listTopics().stream().map(TopicListing::name).toList());
        var batches = batchCells(reconciliation, cells, minIsrByTopic, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Batches {}", idsOf2(batches));

        var bestBatch = pickBestBatchForRestart(batches);
        LOGGER.debugCr(reconciliation, "Best batch {}", idsOf(bestBatch));
        return bestBatch;
    }

    /**
     * Partition the given {@code brokers}
     * into cells that can be rolled in parallel because they
     * contain no replicas in common.
     */
    static List<Set<KafkaNode>> cells(Reconciliation reconciliation,
                                      Collection<KafkaNode> brokers) {

        // find brokers that are individually rollable
        var rollable = brokers.stream().collect(Collectors.toCollection(() ->
                new TreeSet<>(Comparator.comparing(KafkaNode::id))));
        if (rollable.size() < 2) {
            return List.of(rollable);
        } else {
            // partition the set under the equivalence relation "shares a partition with"
            Set<Set<KafkaNode>> disjoint = partitionByHasAnyReplicasInCommon(reconciliation, rollable);
            // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
            // We find the biggest set of brokers which can parallel-rolled
            return disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
        }
    }

    private static Set<Set<KafkaNode>> partitionByHasAnyReplicasInCommon(Reconciliation reconciliation, Set<KafkaNode> rollable) {
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
     */
    static List<Set<KafkaNode>> batchCells(Reconciliation reconciliation,
                                           List<Set<KafkaNode>> cells,
                                           Map<String, Integer> minIsrByTopic,
                                           int maxBatchSize) {
        List<Set<KafkaNode>> result = new ArrayList<>();
        Set<KafkaNode> unavail = new HashSet<>();
        for (var cell : cells) {
            List<Set<KafkaNode>> availBatches = new ArrayList<>();
            for (var kafkaNode : cell) {
                if (!Availability.anyReplicaWouldBeUnderReplicated(kafkaNode, minIsrByTopic)) {
                    LOGGER.debugCr(reconciliation, "No replicas of node {} will be unavailable => add to batch",
                            kafkaNode.id());
                    var currentBatch = availBatches.isEmpty() ? null : availBatches.get(availBatches.size() - 1);
                    if (currentBatch == null || currentBatch.size() >= maxBatchSize) {
                        currentBatch = new HashSet<>();
                        availBatches.add(currentBatch);
                    }
                    currentBatch.add(kafkaNode);
                } else {
                    LOGGER.debugCr(reconciliation, "Some replicas of node {} will be unavailable => do not add to batch", kafkaNode.id());
                    unavail.add(kafkaNode);
                }
            }
            result.addAll(availBatches);
        }
        if (result.isEmpty() && !unavail.isEmpty()) {
            LOGGER.warnCr(reconciliation, "Cannot restart nodes {} without violating some topics' min.in.sync.replicas", idsOf(unavail));
        }
        return result;
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

    /** Returns a new set that is the union of each of the sets in the given {@code merge}. I.e. flatten without duplicates. */
    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }



    private static String idsOf2(Collection<? extends Collection<KafkaNode>> merge) {
        return merge.stream()
                .map(Batching::idsOf)
                .collect(Collectors.joining(",", "{", "}"));
    }

    /**
     * Pick the "best" batch to be restarted.
     * This is the largest batch of available servers
     * @return the "best" batch to be restarted
     */
    static Set<KafkaNode> pickBestBatchForRestart(List<Set<KafkaNode>> batches) {
        var sorted = batches.stream().sorted(Comparator.comparing(Set::size)).toList();
        if (sorted.size() == 0) {
            return Set.of();
        }
        return sorted.get(sorted.size() - 1);
    }



}
