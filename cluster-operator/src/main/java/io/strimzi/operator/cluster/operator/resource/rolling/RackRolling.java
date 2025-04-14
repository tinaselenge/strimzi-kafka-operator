/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
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
    private final List<Context> contexts;
    private final Map<Integer, NodeRef> nodeMap;

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

    private static boolean avail(KafkaNode kafkaNode,
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
                if (avail(kafkaNode, minIsrByTopic)) {
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
     * This is the largest batch of available servers excluding the
     * @return the "best" batch to be restarted
     */
    static Set<KafkaNode> pickBestBatchForRestart(List<Set<KafkaNode>> batches, int controllerId) {
        var sorted = batches.stream().sorted(Comparator.comparing(Set::size)).toList();
        if (sorted.size() == 0) {
            return Set.of();
        }
        if (sorted.size() > 1
                && sorted.get(0).stream().anyMatch(s -> s.id() == controllerId)) {
            return sorted.get(1);
        }
        return sorted.get(0);
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
     * @param nodesNeedingRestart The ids of the nodes which need to be restarted
     * @param maxRestartBatchSize The maximum allowed size for a batch
     * @return The nodes corresponding to a subset of {@code nodeIdsNeedingRestart} that can safely be rolled together
     */
    private static Set<KafkaNode> nextBatch(Reconciliation reconciliation,
                                            RollClient rollClient,
                                            Map<Integer, NodeRef> nodeMap,
                                            Map<Integer, NodeRef> nodesNeedingRestart,
                                            int maxRestartBatchSize) {
        enum NodeFlavour {
            NON_ACTIVE_PURE_CONTROLLER, // A pure KRaft controller node (not a broker) that is not the active controller
            ACTIVE_PURE_CONTROLLER, // A pure KRaft controllers node (not a broker) that is the active controller
            NON_ACTIVE_BROKERISH, // A KRaft or ZooKeeper node that is at least a broker (might be a
            // KRafter combined node that is not the active controller)
            ACTIVE_BROKERISH // A KRaft or ZooKeeper node that is a broker and also the active controller
        }

        int controllerId = rollClient.activeController();
        var partitioned = nodesNeedingRestart.entrySet().stream().collect(Collectors.groupingBy(entry -> {
            NodeRef nodeRef = entry.getValue();
            boolean isActiveController = entry.getKey() == controllerId;
            boolean isPureController = nodeRef.controller() && !nodeRef.broker();
            if (isPureController) {
                if (!isActiveController) {
                    return NodeFlavour.NON_ACTIVE_PURE_CONTROLLER;
                } else {
                    return NodeFlavour.ACTIVE_PURE_CONTROLLER;
                }
            } else { //combined, or pure broker
                if (!isActiveController) {
                    return NodeFlavour.NON_ACTIVE_BROKERISH;
                } else {
                    return NodeFlavour.ACTIVE_BROKERISH;
                }
            }
        }));
        // FIXME The non-nextBatchNonActiveBrokerish branches are not testing for minISR availability
        if (partitioned.get(NodeFlavour.NON_ACTIVE_PURE_CONTROLLER) != null) {
            NodeRef value = partitioned.get(NodeFlavour.NON_ACTIVE_PURE_CONTROLLER).get(0).getValue();
            return Set.of(new KafkaNode(value.nodeId(), value.controller(), value.broker(), Set.of()));
        } else if (partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER) != null) {
            NodeRef value = partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER).get(0).getValue();
            return Set.of(new KafkaNode(value.nodeId(), value.controller(), value.broker(), Set.of()));
        } else if (partitioned.get(NodeFlavour.NON_ACTIVE_BROKERISH) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.NON_ACTIVE_BROKERISH).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextBatchNonActiveBrokerish(reconciliation, rollClient, nodeMap, nodesNeedingRestart, maxRestartBatchSize, controllerId);
        } else if (partitioned.get(NodeFlavour.ACTIVE_BROKERISH) != null) {
            NodeRef value = partitioned.get(NodeFlavour.ACTIVE_BROKERISH).get(0).getValue();
            return Set.of(new KafkaNode(value.nodeId(), value.controller(), value.broker(), Set.of()));
        } else {
            throw new RuntimeException();
        }
    }

    private static Set<KafkaNode> nextBatchNonActiveBrokerish(Reconciliation reconciliation,
                                                              RollClient rollClient,
                                                              Map<Integer, NodeRef> nodeMap,
                                                              Map<Integer, NodeRef> nodesNeedingRestart,
                                                              int maxRestartBatchSize,
                                                              int controllerId) {
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
                                NodeRef nodeRef = nodeMap.get(replicatingBroker.id());
                                return new KafkaNode(replicatingBroker.id(), nodeRef.controller(), nodeRef.broker(), new HashSet<>());
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
        nodesNeedingRestart.forEach((nodeId, nodeRef) -> {
            nodeIdToKafkaNode.putIfAbsent(nodeId, new KafkaNode(nodeId, nodeRef.controller(), nodeRef.broker(), Set.of()));
        });

        // TODO somewhere in here we need to take account of partition reassignments
        //      e.g. if a partition is being reassigned we expect its ISR to change
        //      (see https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment#KIP455:CreateanAdministrativeAPIforReplicaReassignment-Algorithm
        //      which guarantees that addingReplicas are honoured before removingReplicas)
        //      If there are any removingReplicas our availability calculation won't account for the fact
        //      that the controller may shrink the ISR during the reassignment.

        // Split the set of all brokers replicating any partition
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

        // TODO what does describe quorum return in ZK mode?
        // TODO what should the return value of activeController be during a migration?
        var bestBatch = pickBestBatchForRestart(batches, controllerId);
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
            throw new MaxRestartsExceededException("Broker " + context.nodeId() + " has been restarted " + maxRestarts + " times");
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
        if (context.numReconfigs() > maxReconfigs) {
            throw new RuntimeException("Too many reconfigs");
        }
        LOGGER.debugCr(reconciliation, "Node {}: Reconfiguring", context);
        rollClient.reconfigureNode(context.nodeRef(), context.brokerConfigDiff(), context.loggingDiff());
        context.transitionTo(State.RECONFIGURED, time);
        LOGGER.debugCr(reconciliation, "Node {}: Reconfigured", context);
        // TODO create kube Event
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
        var brokerConfigs = rollClient.describeBrokerConfigs(contexts.stream()
                .map(Context::nodeRef).toList());

        var xxx = contexts.stream().collect(Collectors.groupingBy(context -> {
            RollClient.Configs configPair = brokerConfigs.get(context.nodeId());
            var diff = new KafkaBrokerConfigurationDiff(reconciliation,
                    configPair.brokerConfigs(),
                    kafkaConfigProvider.apply(context.nodeId()),
                    kafkaVersion,
                    context.nodeId());
            // TODO what is the source of truth about reconfiguration
            //      on the one hand we have the RestartReason, which might be a singleton of reconfig
            //      on the other hand there is the diff of current vs desired configs
            var loggingDiff = new KafkaBrokerLoggingConfigurationDiff(reconciliation, configPair.brokerLoggerConfigs(), desiredLogging);
            context.brokerConfigDiff(diff);
            context.loggingDiff(loggingDiff);
            if (!diff.isEmpty() && diff.canBeUpdatedDynamically()) {
                return Plan.RECONFIGURE;
            } else if (diff.isEmpty()) {
                return Plan.RECONFIGURE;
            } else {
                return Plan.RESTART;
            }
        }));

        return Map.of(
                Plan.RESTART, Stream.concat(byPlan.getOrDefault(Plan.RESTART, List.of()).stream(), xxx.getOrDefault(Plan.RESTART, List.of()).stream()).toList(),
                Plan.RECONFIGURE, xxx.getOrDefault(Plan.RECONFIGURE, List.of()),
                Plan.RESTART_FIRST, xxx.getOrDefault(Plan.RESTART_FIRST, List.of())
        );
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, RollClient rollClient, NodeRef nodeRef) {
        State state = State.NOT_READY;
        var nodeState = platformClient.nodeState(nodeRef);
        LOGGER.debugCr(reconciliation, "Node {}: nodeState is {}", nodeRef, nodeState);
        switch (nodeState) {
            case NOT_RUNNING:
                state = State.NOT_READY;
                break;
            case NOT_READY:
                state = State.NOT_READY;
                break;
            case READY:
            default:
                try {
                    var bs = rollClient.getBrokerState(nodeRef);
                    LOGGER.debugCr(reconciliation, "Node {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() < BrokerState.RUNNING.value()) {
                        state = State.RECOVERING;
                    } else if (bs.value() == BrokerState.RUNNING.value()) {
                        state = State.SERVING;
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
     * Do a rolling restart (or reconfigure) of some of the Kafka servers given in {@code nodes}.
     * Servers that are not ready (in the Kubernetes sense) will always be considered for restart before any others.
     * The given {@code predicate} will be called for each of the remaining servers and those for which the function returns a non-empty
     * list of reasons will be restarted or reconfigured.
     * When a server is restarted this method guarantees to wait for it to enter the running broker state and
     * become the leader of all its preferred replicas.
     * If a server is not restarted by this method (because the {@code predicate} function returned empty), then
     * it may not be the leader of all its preferred replicas.
     * If this method completes normally then all initially unready servers and the servers for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no servers) will have been successfully restarted/reconfigured.
     * In other words, successful return from this method indicates that all servers seem to be up and
     * "functioning normally".
     * If a server fails to restart or recover its logs within a certain time this method will throw TimeoutException.
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
     * @param rollClient The roll client.
     * @param nodes The nodes (not all of which may need restarting).
     * @param predicate The predicate used to determine whether to restart a particular node.
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
                                      RollClient rollClient,
                                      Collection<NodeRef> nodes,
                                      Function<Integer, RestartReasons> predicate,
                                      Reconciliation reconciliation,
                                      KafkaVersion kafkaVersion,
                                      Function<Integer, String> kafkaConfigProvider,
                                      String desiredLogging,
                                      long postReconfigureTimeoutMs,
                                      long postRestartTimeoutMs,
                                      int maxRestartBatchSize,
                                      int maxRestarts)
            throws ExecutionException, TimeoutException, InterruptedException {

        final var nodeMap = nodes.stream().collect(Collectors.toUnmodifiableMap(NodeRef::nodeId, nodeRef -> nodeRef));
        var contexts = nodes.stream().map(node -> Context.start(node, predicate, time)).toList();
        return new RackRolling(time,
                platformClient,
                rollClient,
                reconciliation,
                kafkaVersion,
                kafkaConfigProvider,
                desiredLogging,
                postReconfigureTimeoutMs,
                postRestartTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                nodeMap,
                contexts);
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final RollClient rollClient;
    private final Reconciliation reconciliation;
    private final KafkaVersion kafkaVersion;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String desiredLogging;
    private final long postReconfigureTimeoutMs;
    private final long postRestartTimeoutMs;
    private final int maxRestartBatchSize;
    private final int maxRestarts;

    public RackRolling(Time time,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       Reconciliation reconciliation,
                       KafkaVersion kafkaVersion,
                       Function<Integer, String> kafkaConfigProvider,
                       String desiredLogging,
                       long postReconfigureTimeoutMs,
                       long postRestartTimeoutMs,
                       int maxRestartBatchSize,
                       int maxRestarts, Map<Integer,
                       NodeRef> nodeMap,
                       List<Context> contexts) {
        this.time = time;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.reconciliation = reconciliation;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.desiredLogging = desiredLogging;
        this.postReconfigureTimeoutMs = postReconfigureTimeoutMs;
        this.postRestartTimeoutMs = postRestartTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.maxRestarts = maxRestarts;
        this.nodeMap = nodeMap;
        this.contexts = contexts;
    }

    public List<Integer> loop() throws TimeoutException, InterruptedException, ExecutionException {

        try {
            LOGGER.debugCr(reconciliation, "Loop");
            // Observe current state and update the contexts
            for (var context : contexts) {
                context.transitionTo(observe(reconciliation, platformClient, rollClient, context.nodeRef()), time);
            }

            int maxReconfigs = 1;
            var byPlan = initialPlan(contexts, maxReconfigs);
            LOGGER.debugCr(reconciliation, "Initial plan: {}", byPlan);
            if (byPlan.getOrDefault(Plan.RESTART_FIRST, List.of()).isEmpty()
                    && byPlan.getOrDefault(Plan.RESTART, List.of()).isEmpty()
                    && byPlan.getOrDefault(Plan.MAYBE_RECONFIGURE, List.of()).isEmpty()) {
                // termination condition met
                LOGGER.debugCr(reconciliation, "Terminate: Empty plan");
                return List.of();
            }

            // Restart any initially unready nodes
            for (var context : byPlan.getOrDefault(Plan.RESTART_FIRST, List.of())) {
                restartNode(reconciliation, time, platformClient, context, maxRestarts);
                long remainingTimeoutMs = awaitState(reconciliation, time, platformClient, rollClient, context, State.SERVING, postRestartTimeoutMs);
                awaitPreferred(reconciliation, time, rollClient, context, remainingTimeoutMs);
                return List.of(context.nodeId());
            }
            // If we get this far we know all nodes are ready

            // Refine the plan, reassigning nodes under MAYBE_RECONFIGURE to either RECONFIGURE or RESTART
            // based on whether they have only reconfiguration config changes
            byPlan = refinePlanForReconfigurability(reconciliation,
                    kafkaVersion,
                    kafkaConfigProvider,
                    desiredLogging,
                    rollClient,
                    byPlan);
            LOGGER.debugCr(reconciliation, "Refined plan: {}", byPlan);
            // Reconfigure any reconfigurable nodes
            for (var context : byPlan.get(Plan.RECONFIGURE)) {
                // TODO decide whether to support parallel/batching dynamic reconfiguration
                // TODO decide whether to support canary reconfiguration for cluster-scoped configs
                reconfigureNode(reconciliation, time, rollClient, context, maxReconfigs);
                time.sleep(postReconfigureTimeoutMs / 2, 0);
                // TODO decide whether we need an explicit healthcheck here
                //      or at least to know that the kube health check probe will have failed at the time
                //      we break to OUTER
                awaitPreferred(reconciliation, time, rollClient, context, postReconfigureTimeoutMs / 2);
                // termination condition
                if (contexts.stream().allMatch(context2 -> context2.state() == State.LEADING_ALL_PREFERRED)) {
                    LOGGER.debugCr(reconciliation, "Terminate: All nodes leading preferred replicas after reconfigure");
                    return List.of();
                }
                return List.of(context.nodeId());
            }

            // If we get this far then all remaining nodes require a restart
            // determine batches of nodes to be restarted together
            var batch = nextBatch(reconciliation, rollClient, nodeMap, byPlan.get(Plan.RESTART).stream().collect(Collectors.toMap(
                    Context::nodeId,
                    context -> nodeMap.get(context.nodeId())
            )), maxRestartBatchSize);
            var batchOfIds = batch.stream().map(KafkaNode::id).collect(Collectors.toSet());
            var batchOfContexts = contexts.stream().filter(context -> batchOfIds.contains(context.nodeId())).collect(Collectors.toSet());
            LOGGER.debugCr(reconciliation, "Restart batch: {}", batchOfContexts);
            // restart a batch
            restartInParallel(reconciliation, time, platformClient, rollClient, batchOfContexts, postRestartTimeoutMs, maxRestarts);

            // termination condition
            if (contexts.stream().allMatch(context -> context.state() == State.LEADING_ALL_PREFERRED)) {
                LOGGER.debugCr(reconciliation, "Terminate: All nodes leading preferred replicas after restart");
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

    private static Map<Plan, List<Context>> initialPlan(List<Context> contexts, int maxReconfigs) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_READY) {
                context.reason().add(RestartReason.POD_UNRESPONSIVE, "Failed rolling health check");
                return Plan.RESTART_FIRST;
            } else {
                var reasons = context.reason();
                if (reasons.getReasons().isEmpty()) {
                    return Plan.NOP;
                } else {
                    if (reasons.singletonOf(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART)
                            && context.numReconfigs() < maxReconfigs) {
                        if (context.numReconfigs() > 0
                                && (context.state() == State.LEADING_ALL_PREFERRED
                                    || context.state() == State.SERVING)) {
                            return Plan.NOP;
                        } else {
//                            context.reason(reasons);
                            return Plan.MAYBE_RECONFIGURE;
                        }
                    } else {
                        if (context.numRestarts() > 0
                            && (context.state() == State.LEADING_ALL_PREFERRED
                                || context.state() == State.SERVING)) {
                            return Plan.NOP;
                        } else {
//                            context.reason(reasons);
                            return Plan.RESTART;
                        }
                    }
                }
            }
        }));
    }

    /* There's a bit of a disconnect between the `predicate` which is passed in, and which will usually be static
     * and the health checks which we do in the observation and planning stages, which are active.

     * Ideally we would invoke the predicate once, union it with the unhealthy brokers and then iterate
     * each iteration unioning with unhealthy brokers
     *
     * We could do that my modelling the result of the predicate as a state
     */


}
