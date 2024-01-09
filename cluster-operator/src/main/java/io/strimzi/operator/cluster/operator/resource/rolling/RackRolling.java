/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RackRolling {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;
    private final Map<Integer, Context> contextMap;
    private static long controllerQuorumFetchTimeout = CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

    private static boolean wouldBeUnderReplicated(Integer minIsr, Replica replica) {
        final boolean wouldByUnderReplicated;
        if (minIsr == null) {
            // if topic doesn't have minISR then it's fine
            wouldByUnderReplicated = false;
        } else {
            // else topic has minISR
            // compute spare = size(ISR) - minISR
            int sizeIsr = replica.isrSize();
            int spare = sizeIsr - minIsr;
            if (spare > 0) {
                // if (spare > 0) then we can restart the broker hosting this replica
                // without the topic being under-replicated
                wouldByUnderReplicated = false;
            } else if (spare == 0) {
                // if we restart this broker this replica would be under-replicated if it's currently in the ISR
                // if it's not in the ISR then restarting the server won't make a difference
                wouldByUnderReplicated = replica.isInIsr();
            } else {
                // this partition is already under-replicated
                // if it's not in the ISR then restarting the server won't make a difference
                // but in this case since it's already under-replicated let's
                // not possible prolong the time to this server rejoining the ISR
                wouldByUnderReplicated = true;
            }
        }
        return wouldByUnderReplicated;
    }

    private static boolean affectsAvailability(KafkaNode kafkaNode,
                                               Map<String, Integer> minIsrByTopic) {
        for (var replica : kafkaNode.replicas()) {
            var topicName = replica.topicName();
            Integer minIsr = minIsrByTopic.get(topicName);
            if (wouldBeUnderReplicated(minIsr, replica)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the majority of the controllers' lastCaughtUpTimestamps are within
     * the controller.quorum.fetch.timeout.ms based on the given quorum info.
     * The given controllerNeedRestarting is the one being considered to restart, therefore excluded from the check.
     */
    private static boolean isQuorumHealthyWithoutNode(Reconciliation reconciliation,
                                                      int controllerNeedRestarting,
                                                      int activeControllerId,
                                                      int totalNumOfControllers,
                                                      Map<Integer, Long> quorumFollowerStates) {
        LOGGER.debugCr(reconciliation, "Determining the impact of restarting controller {} on quorum health", controllerNeedRestarting);
        if (activeControllerId < 0) {
            LOGGER.warnCr(reconciliation, "No controller quorum leader is found because the leader id is set to {}", activeControllerId);
            return false;
        }

        if (totalNumOfControllers == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with a single node. The cluster may be " +
                    "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
            return true;
        }

        long leaderLastCaughtUpTimestamp = quorumFollowerStates.get(activeControllerId);

        long numOfCaughtUpControllers = quorumFollowerStates.entrySet().stream().filter(entry -> {
            int nodeId = entry.getKey();
            long lastCaughtUpTimestamp = entry.getValue();
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", nodeId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", nodeId, lastCaughtUpTimestamp);
                if (nodeId == activeControllerId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < controllerQuorumFetchTimeout) {
                    if (nodeId != controllerNeedRestarting) {
                        return true;
                    }
                    LOGGER.debugCr(reconciliation, "Controller {} has caught up with the controller quorum leader", nodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "Controller {} has fallen behind the controller quorum leader", nodeId);
                }
            }
            return false;
        }).count();

        if (totalNumOfControllers == 2) {
            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. The cluster may be " +
                        "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            return numOfCaughtUpControllers >= (totalNumOfControllers + 2) / 2;
        }
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
                if (affectsAvailability(kafkaNode, minIsrByTopic)) {
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
            throw new UnrestartableNodesException("Cannot restart nodes " + idsOf(unavail) + " without violating some topics' min.in.sync.replicas");
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

    private static String idsOf2(Collection<? extends Collection<KafkaNode>> merge) {
        return merge.stream()
                .map(RackRolling::idsOf)
                .collect(Collectors.joining(",", "{", "}"));
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
            var sorted = disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
            return sorted;
        }
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

    /**
     * Figures out a batch of nodes that can be restarted together.
     * This method enforces the following roll order:
     * <ol>
     *     <li>Pure non-leader controller</li>
     *     <li>Pure leader controller</li>
     *     <li>Non-controller broker (only this case is parallelizable)</li>
     *     <li>Controller broker</li>
     * </ol>
     *
     * @param rollClient The roll client
     * @param contextMap The ids of the nodes in the cluster mapped to its context
     * @param nodesNeedingRestart The ids of the nodes which need to be restarted
     * @param maxRestartBatchSize The maximum allowed size for a batch
     * @return The nodes corresponding to a subset of {@code nodeIdsNeedingRestart} that can safely be rolled together
     */
    private static Set<KafkaNode> nextBatch(Reconciliation reconciliation,
                                            RollClient rollClient,
                                            Map<Integer, Context> contextMap,
                                            Map<Integer, NodeRoles> nodesNeedingRestart,
                                            int totalNumOfControllers,
                                            int maxRestartBatchSize) {
        enum NodeFlavour {
            NON_ACTIVE_PURE_CONTROLLER, // A pure KRaft controller node that is not the active controller
            ACTIVE_PURE_CONTROLLER, // A pure KRaft controllers node that is the active controller
            BROKER_AND_NOT_ACTIVE_CONTROLLER, // A node that is at least a broker and might be a
            // controller (combined node) but that is not the active controller
            BROKER_AND_ACTIVE_CONTROLLER // A node that is a broker and also the active controller
        }

        Map<Integer, Long> quorumState = rollClient.quorumLastCaughtUpTimestamps();
        int activeControllerId = rollClient.activeController();
        var partitioned = nodesNeedingRestart.entrySet().stream().collect(Collectors.groupingBy(entry -> {
            NodeRoles nodeRoles = entry.getValue();
            boolean isActiveController = entry.getKey() == activeControllerId;
            boolean isPureController = nodeRoles.controller() && !nodeRoles.broker();
            if (isPureController) {
                if (isActiveController) {
                    return NodeFlavour.ACTIVE_PURE_CONTROLLER;
                } else {
                    return NodeFlavour.NON_ACTIVE_PURE_CONTROLLER;
                }
            } else { //combined, or pure broker
                if (isActiveController) {
                    return NodeFlavour.BROKER_AND_ACTIVE_CONTROLLER;
                } else {
                    return NodeFlavour.BROKER_AND_NOT_ACTIVE_CONTROLLER;
                }
            }
        }));


        if(activeControllerId > -1) {
            var nodeConfigs = rollClient.describeControllerConfigs(List.of(contextMap.get(activeControllerId).nodeRef()));
            Configs configs = nodeConfigs.get(activeControllerId);
            if (configs != null) {
                ConfigEntry controllerQuorumFetchTimeoutConfig = configs.nodeConfigs().get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME);
                controllerQuorumFetchTimeout = controllerQuorumFetchTimeoutConfig != null ? Long.parseLong(controllerQuorumFetchTimeoutConfig.value()) : CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;
            }
        }

        if (partitioned.get(NodeFlavour.NON_ACTIVE_PURE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.NON_ACTIVE_PURE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextController(reconciliation, nodesNeedingRestart, activeControllerId, totalNumOfControllers, quorumState);

        } else if (partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextController(reconciliation, nodesNeedingRestart, activeControllerId, totalNumOfControllers, quorumState);

        } else if (partitioned.get(NodeFlavour.BROKER_AND_NOT_ACTIVE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.BROKER_AND_NOT_ACTIVE_CONTROLLER).stream()
                    .filter(entry -> entry.getValue().controller() ? isQuorumHealthyWithoutNode(reconciliation, entry.getKey(), activeControllerId, totalNumOfControllers, quorumState) : true)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextBatchBrokers(reconciliation, rollClient, contextMap, nodesNeedingRestart, maxRestartBatchSize);

        } else if (partitioned.get(NodeFlavour.BROKER_AND_ACTIVE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.BROKER_AND_ACTIVE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (nextController(reconciliation, nodesNeedingRestart, activeControllerId, totalNumOfControllers, quorumState).isEmpty()) {
                return Set.of();
            } else {
                return nextBatchBrokers(reconciliation, rollClient, contextMap, nodesNeedingRestart, 1);
            }

        } else {
            throw new RuntimeException();
        }
    }

    /**
     * Returns the next controller that can be restarted without impacting the quorum health.
     */
    private static Set<KafkaNode> nextController(Reconciliation reconciliation,
                                            Map<Integer, NodeRoles> nodesNeedingRestart,
                                            int activeControllerId,
                                            int totalNumOfControllers,
                                            Map<Integer, Long> quorumState) {
        KafkaNode controllerToRestart = null;

        for (int nodeId : nodesNeedingRestart.keySet()) {
            if (isQuorumHealthyWithoutNode(reconciliation, nodeId, activeControllerId, totalNumOfControllers, quorumState)) {
                controllerToRestart = new KafkaNode(nodeId, true, false, Set.of());
                break;
            }
        }

        if (controllerToRestart != null) {
            return Set.of(controllerToRestart);
        } else {
            return Set.of();
        }
    }

    /**
     * Returns a batch of broker nodes that have no topic partitions in common and have no impact on cluster availability if restarted.
     */
    private static Set<KafkaNode> nextBatchBrokers(Reconciliation reconciliation,
                                                              RollClient rollClient,
                                                              Map<Integer, Context> contextMap,
                                                              Map<Integer, NodeRoles> nodesNeedingRestart,
                                                              int maxRestartBatchSize) {
        Map<Integer, KafkaNode> nodeIdToKafkaNode = new HashMap<>();

        // Get all the topics in the cluster
        Collection<TopicListing> topicListings = rollClient.listTopics();

        // batch the describeTopics requests to avoid trying to get the state of all topics in the cluster
        var topicIds = topicListings.stream().map(TopicListing::topicId).toList();

        // Convert the TopicDescriptions to the Server and Replicas model
        List<TopicDescription> topicDescriptions = rollClient.describeTopics(topicIds);

        topicDescriptions.forEach(topicDescription -> {
            topicDescription.partitions().forEach(partition -> {
                partition.replicas().forEach(replicatingBroker -> {
                    var kafkaNode = nodeIdToKafkaNode.computeIfAbsent(replicatingBroker.id(),
                            ig -> {
                                NodeRoles nodeRoles = contextMap.get(replicatingBroker.id()).nodeRoles();
                                return new KafkaNode(replicatingBroker.id(), nodeRoles.controller(), nodeRoles.broker(), new HashSet<>());
                            });
                    kafkaNode.replicas().add(new Replica(
                            replicatingBroker,
                            topicDescription.name(),
                            partition.partition(),
                            partition.isr()));
                });
            });
        });

        // Add any servers which we know about but which were absent from any partition metadata
        // i.e. brokers without any assigned partitions
        nodesNeedingRestart.forEach((nodeId, nodeRoles) -> {
            nodeIdToKafkaNode.putIfAbsent(nodeId, new KafkaNode(nodeId, nodeRoles.controller(), nodeRoles.broker(), Set.of()));
        });

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

        var minIsrByTopic = rollClient.describeTopicMinIsrs(topicListings.stream().map(TopicListing::name).toList());
        var batches = batchCells(reconciliation, cells, minIsrByTopic, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Batches {}", idsOf2(batches));

        var bestBatch = pickBestBatchForRestart(batches);
        LOGGER.debugCr(reconciliation, "Best batch {}", idsOf(bestBatch));
        return bestBatch;
    }

    public static void main(String[] a) {
        int numRacks = 3;
        int numBrokers = 10;
        int numControllers = 5;
        int numTopics = 1;
        int numPartitionsPerTopic = 10;
        boolean coloControllers = true;
        int rf = 3;
        System.out.printf("numRacks = %d%n", numRacks);
        System.out.printf("numBrokers = %d%n", numBrokers);
        System.out.printf("numTopics = %d%n", numTopics);
        System.out.printf("numPartitionsPerTopic = %d%n", numPartitionsPerTopic);
        System.out.printf("rf = %d%n", rf);

        List<KafkaNode> kafkaNodes = new ArrayList<>();
        for (int serverId = 0; serverId < (coloControllers ? Math.max(numBrokers, numControllers) : numControllers + numBrokers); serverId++) {
            KafkaNode kafkaNode = new KafkaNode(serverId, false, true, new LinkedHashSet<>());
            kafkaNodes.add(kafkaNode);
            boolean isController = serverId < numControllers;
            if (isController) {
                kafkaNode.replicas().add(new Replica("__cluster_metadata", 0, (short) numControllers));
            }
        }

        for (int topic = 1; topic <= numTopics; topic++) {
            for (int partition = 0; partition < numPartitionsPerTopic; partition++) {
                for (int replica = partition; replica < partition + rf; replica++) {
                    KafkaNode broker = kafkaNodes.get((coloControllers ? 0 : numControllers) + replica % numBrokers);
                    broker.replicas().add(new Replica("t" + topic, partition, (short) rf));
                }
            }
        }

        for (var broker : kafkaNodes) {
            System.out.println(broker);
        }

        // TODO validate

        var results = cells(Reconciliation.DUMMY_RECONCILIATION, kafkaNodes);

        int group = 0;
        for (var result : results) {
            System.out.println("Group " + group + ": " + result.stream().map(KafkaNode::id).collect(Collectors.toCollection(TreeSet::new)));
            group++;
        }
    }

    static String podName(NodeRef nodeRef) {
        return nodeRef.podName();
    }

    private static void restartNode(Reconciliation reconciliation,
                                    Time time,
                                    PlatformClient platformClient,
                                    Context context,
                                    int maxRestarts) {
        if (context.numRestarts() >= maxRestarts) {
            throw new MaxRestartsExceededException("Node " + context.nodeId() + " has been restarted " + maxRestarts + " times");
        }
        LOGGER.debugCr(reconciliation, "Node {}: Restarting", context);
        platformClient.restartNode(context.nodeRef());
        context.transitionTo(State.RESTARTED, time);
        LOGGER.debugCr(reconciliation, "Node {}: Restarted", context);
        // TODO kube create an Event with the context.reason
    }

    private static void reconfigureNode(Reconciliation reconciliation,
                                        Time time,
                                        RollClient rollClient,
                                        Context context,
                                        int maxReconfigs) {
        if (context.numReconfigs() >= maxReconfigs) {
            context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
            throw new RuntimeException("Too many reconfigs");
        }
        LOGGER.debugCr(reconciliation, "Node {}: Reconfiguring", context);
        rollClient.reconfigureNode(context.nodeRef(), context.brokerConfigDiff(), context.loggingDiff());
        context.transitionTo(State.RECONFIGURED, time);
        LOGGER.debugCr(reconciliation, "Node {}: Reconfigured", context);
    }


    private static long awaitState(Reconciliation reconciliation,
                                   Time time,
                                   PlatformClient platformClient,
                                   RollClient rollClient,
                                   Context context,
                                   State targetState,
                                   long timeoutMs) throws TimeoutException {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to enter state {}", context, targetState);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + targetState + " within " + timeoutMs + " ms: " + context
        ).poll(1_000, () -> {
            var state = context.transitionTo(observe(reconciliation, platformClient, rollClient, context.nodeRef()), time);
            return state == targetState;
        });
    }

    private static long awaitPreferred(Reconciliation reconciliation,
                                       Time time,
                                       RollClient rollClient,
                                       Context context,
                                       long timeoutMs) throws TimeoutException {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to be leader of all its preferred replicas", context);
        return Alarm.timer(time,
                timeoutMs,
                () -> "Failed to reach " + State.LEADING_ALL_PREFERRED + " within " + timeoutMs + ": " + context)
        .poll(1_000, () -> {
            var remainingReplicas = rollClient.tryElectAllPreferredLeaders(context.nodeRef());
            if (remainingReplicas == 0) {
                context.transitionTo(State.LEADING_ALL_PREFERRED, time);
            }
            return remainingReplicas == 0;
        });
    }

    private static void restartInParallel(Reconciliation reconciliation, Time time, PlatformClient platformClient, RollClient rollClient, Set<Context> batch, long timeoutMs, int maxRestarts) throws TimeoutException {
        for (Context context : batch) {
            restartNode(reconciliation, time, platformClient, context, maxRestarts);
        }
        long remainingTimeoutMs = timeoutMs;
        for (Context context : batch) {
            remainingTimeoutMs = awaitState(reconciliation, time, platformClient, rollClient, context, State.SERVING, remainingTimeoutMs);
        }

        var serverContextWrtIds = new HashMap<Integer, Context>();
        var nodeRefs = new ArrayList<NodeRef>();
        for (Context context : batch) {
            Integer id = context.nodeId();
            nodeRefs.add(context.nodeRef());
            serverContextWrtIds.put(id, context);
        }

        Alarm.timer(time,
                remainingTimeoutMs,
                () -> "Servers " + nodeRefs + " failed to reach " + State.LEADING_ALL_PREFERRED + " within " + timeoutMs + ": " +
                        nodeRefs.stream().map(nodeRef -> serverContextWrtIds.get(nodeRef.nodeId())).collect(Collectors.toSet()))
            .poll(1_000, () -> {
                var toRemove = new ArrayList<NodeRef>();
                for (var nodeRef : nodeRefs) {
                    if (rollClient.tryElectAllPreferredLeaders(nodeRef) == 0) {
                        serverContextWrtIds.get(nodeRef.nodeId()).transitionTo(State.LEADING_ALL_PREFERRED, time);
                        toRemove.add(nodeRef);
                    }
                }
                nodeRefs.removeAll(toRemove);
                return nodeRefs.isEmpty();
            });
    }

    private static Map<Plan, List<Context>> refinePlanForReconfigurability(Reconciliation reconciliation,
                                                                           KafkaVersion kafkaVersion,
                                                                           Function<Integer, String> kafkaConfigProvider,
                                                                           String desiredLogging,
                                                                           RollClient rollClient,
                                                                           Map<Plan, List<Context>> byPlan) {
        var contexts = byPlan.getOrDefault(Plan.MAYBE_RECONFIGURE, List.of());
        var nodeConfigs = rollClient.describeBrokerConfigs(contexts.stream()
                .map(Context::nodeRef).toList());

        var refinedPlan = contexts.stream().collect(Collectors.groupingBy(context -> {
            Configs configPair = nodeConfigs.get(context.nodeId());

            var diff = new KafkaBrokerConfigurationDiff(reconciliation,
                    configPair.nodeConfigs(),
                    kafkaConfigProvider.apply(context.nodeId()),
                    kafkaVersion,
                    context.nodeRef());
            var loggingDiff = new KafkaBrokerLoggingConfigurationDiff(reconciliation, configPair.nodeLoggerConfigs(), desiredLogging);
            context.brokerConfigDiff(diff);
            context.loggingDiff(loggingDiff);
            if (!diff.isEmpty() && diff.canBeUpdatedDynamically()) {
                return Plan.RECONFIGURE;
            } else if (diff.isEmpty()) {
                return Plan.NOP;
            } else {
                context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                return Plan.RESTART;
            }
        }));

        return Map.of(
                Plan.RESTART, Stream.concat(byPlan.getOrDefault(Plan.RESTART, List.of()).stream(), refinedPlan.getOrDefault(Plan.RESTART, List.of()).stream()).toList(),
                Plan.RECONFIGURE, refinedPlan.getOrDefault(Plan.RECONFIGURE, List.of()),
                Plan.NOP, refinedPlan.getOrDefault(Plan.NOP, List.of()),
                Plan.RESTART_FIRST, refinedPlan.getOrDefault(Plan.RESTART_FIRST, List.of())
        );
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, RollClient rollClient, NodeRef nodeRef) {
        State state;
        var nodeState = platformClient.nodeState(nodeRef);
        LOGGER.debugCr(reconciliation, "Node {}: nodeState is {}", nodeRef, nodeState);
        switch (nodeState) {
            case NOT_RUNNING:
                state = State.NOT_RUNNING;
                break;
            case READY:
                state = State.SERVING;
                break;
            case NOT_READY:
            default:
                try {
                    var bs = rollClient.getBrokerState(nodeRef);
                    LOGGER.debugCr(reconciliation, "Node {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() == BrokerState.RECOVERY.value()) {
                        state = State.RECOVERING;
                    } else {
                        state = State.NOT_READY;
                    }
                } catch (Exception e) {
                    state = State.NOT_READY;
                }
        }
        LOGGER.debugCr(reconciliation, "Node {}: observation outcome is {}", nodeRef, state);
        return state;
    }

    enum Plan {
        // Used for brokers that are initially healthy and require neither restart not reconfigure
        NOP,
        // Used for brokers that are initially not healthy
        RESTART_FIRST,
        // Used in {@link #initialPlan(Function, List, int)} for brokers that require reconfigure
        // before we know whether the actual config changes are reconfigurable
        MAYBE_RECONFIGURE,
        // Used in {@link #refinePlanForReconfigurability(Reconciliation, KafkaVersion, Function, String, RollClient, Map)}
        // once we know a MAYBE_RECONFIGURE node can actually be reconfigured
        RECONFIGURE,
        RESTART
    }

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * The expected worst case execution time of this function is approximately
     * {@code (timeoutMs * maxRestarts + postReconfigureTimeoutMs) * size(nodes)}.
     * This is reached when:
     * <ol>
     *     <li>We initially attempt to reconfigure all nodes</li>
     *     <li>Those reconfigurations all fail, so we resort to restarts</li>
     *     <li>We require {@code maxRestarts} restarts for each node, and each restart uses the
     *         maximum {@code timeoutMs}.</li>
     * </ol>
     *
     * @param platformClient The platform client.
     * @param nodes The nodes (not all of which may need restarting).
     * @param predicate The predicate used to determine whether to restart a particular node.
     * @param clusterCaCertSecret   Secret with the Cluster CA public key
     * @param coKeySecret           Secret with the Cluster CA private key
     * @param adminClientProvider   Kafka Admin client provider
     * @param postReconfigureTimeoutMs The maximum time to wait after a reconfiguration.
     * @param postRestartTimeoutMs The maximum time to wait after a restart.
     * @param maxRestartBatchSize The maximum number of servers that might be restarted at once.
     * @param maxRestarts The maximum number of restarts attempted for any individual server
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public static RackRolling rollingRestart(Time time,
                                      PlatformClient platformClient,
                                      Collection<NodeRef> nodes,
                                      Function<Integer, RestartReasons> predicate,
                                      Secret clusterCaCertSecret,
                                      Secret coKeySecret,
                                      AdminClientProvider adminClientProvider,
                                      Reconciliation reconciliation,
                                      KafkaVersion kafkaVersion,
                                      boolean allowReconfiguration,
                                      Function<Integer, String> kafkaConfigProvider,
                                      String desiredLogging,
                                      long postReconfigureTimeoutMs,
                                      long postRestartTimeoutMs,
                                      int maxRestartBatchSize,
                                      int maxRestarts,
                                      int maxReconfigs)
            throws ExecutionException, TimeoutException, InterruptedException {
        final var contextMap = nodes.stream().collect(Collectors.toUnmodifiableMap(node -> node.nodeId(), node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)));
        RollClient rollClient = new RollClientImpl(new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), clusterCaCertSecret, coKeySecret));
        return new RackRolling(time,
                platformClient,
                rollClient,
                reconciliation,
                clusterCaCertSecret,
                coKeySecret,
                adminClientProvider,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                desiredLogging,
                postReconfigureTimeoutMs,
                postRestartTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                contextMap);
    }

    // visible for testing
    protected static RackRolling rollingRestart(Time time,
                                                PlatformClient platformClient,
                                                RollClient rollClient,
                                                Collection<NodeRef> nodes,
                                                Function<Integer, RestartReasons> predicate,
                                                Reconciliation reconciliation,
                                                KafkaVersion kafkaVersion,
                                                AdminClientProvider adminClientProvider,
                                                boolean allowReconfiguration,
                                                Function<Integer, String> kafkaConfigProvider,
                                                String desiredLogging,
                                                long postReconfigureTimeoutMs,
                                                long postRestartTimeoutMs,
                                                int maxRestartBatchSize,
                                                int maxRestarts,
                                                int maxReconfigs)
            throws ExecutionException, TimeoutException, InterruptedException {
        final var contextMap = nodes.stream().collect(Collectors.toUnmodifiableMap(node -> node.nodeId(), node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)));

        return new RackRolling(time,
                platformClient,
                rollClient,
                reconciliation,
                null,
                null,
                adminClientProvider,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                desiredLogging,
                postReconfigureTimeoutMs,
                postRestartTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                contextMap);
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final RollClient rollClient;
    private final Reconciliation reconciliation;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final AdminClientProvider adminClientProvider;
    private final KafkaVersion kafkaVersion;
    private final boolean allowReconfiguration;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String desiredLogging;
    private final long postReconfigureTimeoutMs;
    private final long postRestartTimeoutMs;
    private final int maxRestartBatchSize;
    private final int maxRestarts;
    private final int maxReconfigs;

    public RackRolling(Time time,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       Reconciliation reconciliation,
                       Secret clusterCaCertSecret,
                       Secret coKeySecret,
                       AdminClientProvider adminClientProvider,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       Function<Integer, String> kafkaConfigProvider,
                       String desiredLogging,
                       long postReconfigureTimeoutMs,
                       long postRestartTimeoutMs,
                       int maxRestartBatchSize,
                       int maxRestarts,
                       int maxReconfigs,
                       Map<Integer, Context> contextMap) {
        this.time = time;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.reconciliation = reconciliation;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.adminClientProvider = adminClientProvider;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.desiredLogging = desiredLogging;
        this.postReconfigureTimeoutMs = postReconfigureTimeoutMs;
        this.postRestartTimeoutMs = postRestartTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.maxRestarts = maxRestarts;
        this.maxReconfigs = maxReconfigs;
        this.contextMap = contextMap;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**
     * Process each context to determine which nodes need restarting.
     * Servers that are not ready (in the Kubernetes sense) will always be considered for restart before any others.
     * The given {@code predicate} will be called for each of the remaining servers and those for which the function returns a non-empty
     * list of reasons will be restarted or reconfigured.
     * When a server is restarted this method guarantees to wait for it to enter the running broker state and
     * become the leader of all its preferred replicas.
     * If a server is not restarted by this method (because the {@code predicate} function returned empty), then
     * it may not be the leader of all its preferred replicas.
     * This method is executed repeatedly until there is no nodes left to restart or reconfigure
     * If this method completes normally then all initially unready servers and the servers for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no servers) will have been successfully restarted/reconfigured.
     * In other words, successful return from this method indicates that all servers seem to be up and
     * "functioning normally".
     * If a server fails to restart or recover its logs within a certain time this method will throw TimeoutException.
     **/
    public List<Integer> loop() throws TimeoutException, InterruptedException, ExecutionException {

        try {
            LOGGER.debugCr(reconciliation, "Loop");

            // Observe current state and update the contexts
            int totalNumOfControllers = 0;
            var contexts = contextMap.values().stream().toList();
            for (var context : contexts) {
                context.transitionTo(observe(reconciliation, platformClient, rollClient, context.nodeRef()), time);
                if (context.nodeRoles().controller()) totalNumOfControllers++;
            }

            var byPlan = initialPlan(contexts);
            LOGGER.debugCr(reconciliation, "Initial plan: {}", byPlan);

            // Restart any initially unready nodes
            if (!byPlan.getOrDefault(Plan.RESTART_FIRST, List.of()).isEmpty()) {
                return restartUnReadyNodes(byPlan.get(Plan.RESTART_FIRST), totalNumOfControllers);
            }

            // If we get this far we know all nodes are ready
            initAdminClients(rollClient, contexts, reconciliation, clusterCaCertSecret, coKeySecret);

            // Refine the plan, reassigning nodes under MAYBE_RECONFIGURE to either RECONFIGURE or RESTART
            // based on whether they have only reconfiguration config changes
            if (allowReconfiguration) {
                byPlan = refinePlanForReconfigurability(reconciliation,
                        kafkaVersion,
                        kafkaConfigProvider,
                        desiredLogging,
                        rollClient,
                        byPlan);
                LOGGER.debugCr(reconciliation, "Refined plan: {}", byPlan);
            }

            // Reconfigure any reconfigurable nodes
            for (var context : byPlan.getOrDefault(Plan.RECONFIGURE, List.of())) {
                // TODO decide whether to support canary reconfiguration for cluster-scoped configs (nice to have)
                try {
                    reconfigureNode(reconciliation, time, rollClient, context, maxReconfigs);
                } catch (RuntimeException e) {
                    return List.of(context.nodeId());
                }

                time.sleep(postReconfigureTimeoutMs / 2, 0);
                // TODO decide whether we need an explicit healthcheck here
                //      or at least to know that the kube health check probe will have failed at the time
                //      we break to OUTER (We need to test a scenario of breaking configuration change, does this sleep catch it?)
                awaitPreferred(reconciliation, time, rollClient, context, postReconfigureTimeoutMs / 2);
                // termination condition
                if (contexts.stream().allMatch(context2 -> context2.state() == State.LEADING_ALL_PREFERRED)) {
                    LOGGER.debugCr(reconciliation, "Terminate: All nodes leading preferred replicas after reconfigure");
                    return List.of();
                }
                return List.of(context.nodeId());
            }

            if (byPlan.getOrDefault(Plan.RESTART, List.of()).isEmpty()) {
                // termination condition met
                // TODO: decide how to handle if there are nodes in RECOVERY state
                LOGGER.debugCr(reconciliation, "Terminate: No Kafka nodes left to reconcile");
                return List.of();
            }

            // If we get this far then all remaining nodes require a restart
            // determine batches of nodes to be restarted together
            var batch = nextBatch(reconciliation, rollClient, contextMap, byPlan.get(Plan.RESTART).stream().collect(Collectors.toMap(
                    Context::nodeId,
                    Context::nodeRoles
            )), totalNumOfControllers, maxRestartBatchSize);
            var batchOfIds = batch.stream().map(KafkaNode::id).collect(Collectors.toSet());
            var batchOfContexts = contexts.stream().filter(context -> batchOfIds.contains(context.nodeId())).collect(Collectors.toSet());
            LOGGER.debugCr(reconciliation, "Restart batch: {}", batchOfContexts);
            // restart a batch
            restartInParallel(reconciliation, time, platformClient, rollClient, batchOfContexts, postRestartTimeoutMs, maxRestarts);

            if (contexts.stream().allMatch(context -> context.state() == State.LEADING_ALL_PREFERRED)) {
                LOGGER.debugCr(reconciliation, "Reconciliation completed successfully: All nodes leading preferred replicas after restart");
                return List.of();
            } else {
                return batchOfIds.stream().toList();
            }

        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (UncheckedExecutionException e) {
            throw e.getCause();
        }
    }

    private void initAdminClients(RollClient rollClient, List<Context> contexts, Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret) {
        try {
            Set<NodeRef> controllerNodes = contexts.stream().filter(c -> c.nodeRoles().controller()).map(n -> n.nodeRef()).collect(Collectors.toSet());
            // TODO update it to the correct service name and port for controllers
            String controllerBootstrapHostnames  = controllerNodes.stream().map(node -> DnsNameGenerator.podDnsName(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName()) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            Admin controllerAdmin = adminClientProvider.createControllerAdminClient(controllerBootstrapHostnames, clusterCaCertSecret, coKeySecret, "cluster-operator");
            rollClient.setControllerAdmin(controllerAdmin);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create admin client for controllers");
        }

        try {
            Set<NodeRef> brokerNodes = contexts.stream().filter(c -> c.nodeRoles().broker()).map(n -> n.nodeRef()).collect(Collectors.toSet());
            String brokerBootstrapHostnames  = brokerNodes.stream().map(node -> DnsNameGenerator.podDnsName(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName()) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            Admin brokerAdmin = adminClientProvider.createAdminClient(brokerBootstrapHostnames, clusterCaCertSecret, coKeySecret, "cluster-operator");
            rollClient.setBrokerAdmin(brokerAdmin);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create admin client for brokers");
        }
    }

    private List<Integer> restartUnReadyNodes(List<Context> contexts, int totalNumOfControllers) throws TimeoutException {
        Set<Context> pureControllerNodesToRestart = new HashSet();
        Set<Context> combinedNodesToRestart = new HashSet<>();
        int numOfPendingCombinedNodes = 0;
        for (var context : contexts.stream().filter(context -> context.nodeRoles().controller()).collect(Collectors.toList())) {
            // restart pure controllers first and then combined nodes
            if (!context.nodeRoles().broker()) {
                pureControllerNodesToRestart.add(context);
            } else {
                if (context.state().equals(State.NOT_RUNNING)) {
                    numOfPendingCombinedNodes++;
                }
                combinedNodesToRestart.add(context);
            }
        }

        if (totalNumOfControllers > 1 && totalNumOfControllers == numOfPendingCombinedNodes) {
            // if all controller nodes (not single node quorum) are combined and all of them are not running e.g. Pending, we need to restart them all at the same time to form the quorum.
            // This is because until the quorum has been formed and broker process can connect to it, the combined nodes do not become ready.
            restartInParallel(reconciliation, time, platformClient, rollClient, combinedNodesToRestart, postRestartTimeoutMs, maxRestarts);
            return combinedNodesToRestart.stream().map(Context::nodeId).toList();
        }

        Context nodeToRestart;
        if (pureControllerNodesToRestart.size() > 0) {
            nodeToRestart = pureControllerNodesToRestart.iterator().next();
        } else if(combinedNodesToRestart.size() > 0) {
            nodeToRestart = combinedNodesToRestart.iterator().next();
        } else {
            nodeToRestart = contexts.get(0);
        }

        restartNode(reconciliation, time, platformClient, nodeToRestart, maxRestarts);
        long remainingTimeoutMs = awaitState(reconciliation, time, platformClient, rollClient, nodeToRestart, State.SERVING, postRestartTimeoutMs);
        awaitPreferred(reconciliation, time, rollClient, nodeToRestart, remainingTimeoutMs);
        return List.of(nodeToRestart.nodeId());
    }

    private static Map<Plan, List<Context>> initialPlan(List<Context> contexts) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_READY || context.state() == State.NOT_RUNNING) {
                context.reason().add(RestartReason.POD_UNRESPONSIVE, "Failed rolling health check");
                return Plan.RESTART_FIRST;
            } else if (context.state() == State.RECOVERING) {
                return Plan.NOP;
            } else {
                var reasons = context.reason();
                if (!reasons.getReasons().isEmpty()) {
                    if (context.numRestarts() > 0
                            && (context.state() == State.LEADING_ALL_PREFERRED
                            || context.state() == State.SERVING)) {
                        return Plan.NOP;
                    } else {
                        return Plan.RESTART;
                    }
                } else {
                    if (context.numReconfigs() > 0
                            && (context.state() == State.LEADING_ALL_PREFERRED
                            || context.state() == State.RECONFIGURED)) {
                        return Plan.NOP;
                    } else {
                        return Plan.MAYBE_RECONFIGURE;
                    }
                }
            }
        }));
    }

}
