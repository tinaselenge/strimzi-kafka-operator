/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RackRolling
 */
@SuppressWarnings({"ParameterNumber" })
public class RackRolling {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;
    private final Map<Integer, Context> contextMap;
    private static long controllerQuorumFetchTimeout = CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

    /**
     * Returns true if the majority of the controllers' lastCaughtUpTimestamps are within
     * the controller.quorum.fetch.timeout.ms based on the given quorum info.
     * The given controllerNeedRestarting is the one being considered to restart, therefore excluded from the check.
     *
     * The total number of controller is passed in to this method rather than using the size of the quorum followers
     * returned from the Admin. This is because when scaling down controllers, the returned quorum info from them
     * could contain inconsistent number of followers.
     */
    private static boolean isQuorumHealthyWithoutNode(Reconciliation reconciliation,
                                                      int controllerNeedRestarting,
                                                      int activeControllerId,
                                                      int controllerCount,
                                                      Map<Integer, Long> quorumFollowerStates) {
        LOGGER.debugCr(reconciliation, "Determining the impact of restarting controller {} on quorum health", controllerNeedRestarting);
        if (activeControllerId < 0) {
            LOGGER.warnCr(reconciliation, "No controller quorum leader is found because the leader id is set to {}", activeControllerId);
            return false;
        }

        if (controllerCount == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on controller quorum with a single node. The cluster may be " +
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

        if (controllerCount == 2) {
            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. The cluster may be " +
                        "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            boolean result =  numOfCaughtUpControllers >= (controllerCount + 2) / 2;
            if (!result) {
                LOGGER.debugCr(reconciliation, "Controller {} cannot be restarted without impacting quorum health", controllerNeedRestarting);
            }
            return result;
        }
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
     * @param totalNumOfControllerNodes The total number of controllers in this cluster
     * @param maxRestartBatchSize The maximum allowed size for a batch
     * @return The nodes corresponding to a subset of {@code nodeIdsNeedingRestart} that can safely be rolled together
     */
    private static Set<KafkaNode> nextBatch(Reconciliation reconciliation,
                                            RollClient rollClient,
                                            Map<Integer, Context> contextMap,
                                            Map<Integer, NodeRoles> nodesNeedingRestart,
                                            int totalNumOfControllerNodes,
                                            int maxRestartBatchSize) {
        enum NodeFlavour {
            NON_ACTIVE_PURE_CONTROLLER, // A pure KRaft controller node that is not the active controller
            ACTIVE_PURE_CONTROLLER, // A pure KRaft controllers node that is the active controller
            COMBINED_AND_NOT_ACTIVE_CONTROLLER, // A combined node that is at least a broker and might be a
            // controller (combined node) but that is not the active controller
            COMBINED_AND_ACTIVE_CONTROLLER, // A combined node that is a broker and also the active controller
            BROKER // A node that is pure Broker
        }

        Map<Integer, Long> quorumState = rollClient.quorumLastCaughtUpTimestamps();
        int activeControllerId = rollClient.activeController();
        LOGGER.debugCr(reconciliation, "The active controller is {}", activeControllerId);

        var partitioned = nodesNeedingRestart.entrySet().stream().collect(Collectors.groupingBy(entry -> {
            NodeRoles nodeRoles = entry.getValue();
            boolean isActiveController = entry.getKey() == activeControllerId;
            boolean isPureController = nodeRoles.controller() && !nodeRoles.broker();
            boolean isPureBroker = !nodeRoles.controller() && nodeRoles.broker();
            if (isPureController) {
                if (isActiveController) {
                    return NodeFlavour.ACTIVE_PURE_CONTROLLER;
                } else {
                    return NodeFlavour.NON_ACTIVE_PURE_CONTROLLER;
                }
            } else { //combined, or pure broker
                if (isActiveController) {
                    return NodeFlavour.COMBINED_AND_ACTIVE_CONTROLLER;
                } else if (isPureBroker) {
                    return NodeFlavour.BROKER;
                } else {
                    return NodeFlavour.COMBINED_AND_NOT_ACTIVE_CONTROLLER;
                }
            }
        }));

        LOGGER.debugCr(reconciliation, "Partitioned nodes: {}", partitioned);

        if (activeControllerId >= 0) {
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
            return nextController(reconciliation, nodesNeedingRestart, activeControllerId, totalNumOfControllerNodes, quorumState);

        } else if (partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.ACTIVE_PURE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextController(reconciliation, nodesNeedingRestart, activeControllerId, totalNumOfControllerNodes, quorumState);

        } else if (partitioned.get(NodeFlavour.COMBINED_AND_NOT_ACTIVE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.COMBINED_AND_NOT_ACTIVE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextNode(reconciliation, rollClient, contextMap, nodesNeedingRestart, activeControllerId, totalNumOfControllerNodes, quorumState);

        } else if (partitioned.get(NodeFlavour.COMBINED_AND_ACTIVE_CONTROLLER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.COMBINED_AND_ACTIVE_CONTROLLER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return nextNode(reconciliation, rollClient, contextMap, nodesNeedingRestart, activeControllerId, totalNumOfControllerNodes, quorumState);

        } else if (partitioned.get(NodeFlavour.BROKER) != null) {
            nodesNeedingRestart = partitioned.get(NodeFlavour.BROKER).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return Batching.nextBatchBrokers(reconciliation, rollClient, contextMap, nodesNeedingRestart, maxRestartBatchSize);
        } else {
            throw new RuntimeException("Nodes did not get partitioned based on their process role: " + nodesNeedingRestart);
        }
    }

    /**
     * @param reconciliation The roll client
     * @param nodesNeedingRestart The ids of the nodes which need to be restarted
     * @param activeControllerId The active controller id
     * @param controllerCount The total number of controllers in this cluster
     * @param quorumState The maximum allowed size for a batch
     * @return The first one from the given list of nodes that can be restarted without impacting the quorum health.
     * If there is no node that doesn't have an impact on the quorum health, an empty set is returned.
     */
    private static Set<KafkaNode> nextController(Reconciliation reconciliation,
                                            Map<Integer, NodeRoles> nodesNeedingRestart,
                                            int activeControllerId,
                                            int controllerCount,
                                            Map<Integer, Long> quorumState) {
        KafkaNode controllerToRestart = null;

        for (int nodeId : nodesNeedingRestart.keySet()) {
            if (isQuorumHealthyWithoutNode(reconciliation, nodeId, activeControllerId, controllerCount, quorumState)) {
                controllerToRestart = new KafkaNode(nodeId, true, false, Set.of());
                break;
            }
        }

        if (controllerToRestart != null) {
            return Set.of(controllerToRestart);
        } else {
            LOGGER.warnCr(reconciliation, "Controllers {} cannot be restarted without impacting the quorum health", nodesNeedingRestart);
            return Set.of();
        }
    }

    private static Set<KafkaNode> nextNode(Reconciliation reconciliation, RollClient rollClient, Map<Integer, Context> contextMap, Map<Integer, NodeRoles> nodesNeedingRestart, int activeControllerId, int controllerCount, Map<Integer, Long> quorumState) {
        KafkaNode brokerToRestart = null;
        Map<Integer, KafkaNode> nodeIdToKafkaNode = Availability.nodeIdToKafkaNode(rollClient, contextMap, nodesNeedingRestart);

        var minIsrByTopic = rollClient.describeTopicMinIsrs(rollClient.listTopics().stream().map(TopicListing::name).toList());

        for (Integer node : nodesNeedingRestart.keySet()) {
            KafkaNode kafkaNode = nodeIdToKafkaNode.get(node);
            if (isQuorumHealthyWithoutNode(reconciliation, node, activeControllerId, controllerCount, quorumState)
                    && !Availability.anyReplicaWouldBeUnderReplicated(kafkaNode, minIsrByTopic)) {
                brokerToRestart = kafkaNode;
                break;
            }
        }

        if (brokerToRestart != null) {
            return Set.of(brokerToRestart);
        } else {
            LOGGER.warnCr(reconciliation, "Combined node {} cannot be restarted without impacting the quorum health", nodesNeedingRestart);
            return Set.of();
        }
    }

    private static void restartNode(Reconciliation reconciliation,
                                    Time time,
                                    PlatformClient platformClient,
                                    Context context,
                                    int maxRestarts) {
        if (context.numRestarts() >= maxRestarts) {
            throw new MaxRestartsExceededException("Node " + context.nodeRef() + " has been restarted " + maxRestarts + " times");
        }
        LOGGER.debugCr(reconciliation, "Node {}: Restarting", context.nodeRef());
        try {
            platformClient.restartNode(context.nodeRef(), context.reason());
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "An exception thrown during the restart of the node {}", context.nodeRef(), e);
        }
        context.transitionTo(State.RESTARTED, time);
        LOGGER.debugCr(reconciliation, "Node {}: Restarted", context.nodeRef());
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
        LOGGER.debugCr(reconciliation, "Node {}: Reconfiguring", context.nodeRef());
        rollClient.reconfigureNode(context.nodeRef(), context.brokerConfigDiff(), context.loggingDiff());
        context.transitionTo(State.RECONFIGURED, time);
        LOGGER.debugCr(reconciliation, "Node {}: Reconfigured", context.nodeRef());
    }


    private static long awaitState(Reconciliation reconciliation,
                                   Time time,
                                   PlatformClient platformClient,
                                   AgentClient agentClient,
                                   Context context,
                                   State targetState,
                                   long timeoutMs) throws TimeoutException {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to enter state {}", context, targetState);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + targetState + " within " + timeoutMs + " ms: " + context
        ).poll(1_000, () -> {
            var state = context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            return state == targetState;
        });
    }

    private static long awaitPreferred(Reconciliation reconciliation,
                                       Time time,
                                       RollClient rollClient,
                                       Context context,
                                       long timeoutMs) {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to be leader of all its preferred replicas", context);
        try {
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
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for node to be leader for all its preferred replicas");
            return 0;
        }
    }

    private void restartInParallel(Reconciliation reconciliation,
                                          Time time,
                                          PlatformClient platformClient,
                                          RollClient rollClient,
                                          AgentClient agentClient,
                                          Set<Context> batch,
                                          long timeoutMs,
                                          int maxRestarts) throws TimeoutException {
        for (Context context : batch) {
            restartNode(reconciliation, time, platformClient, context, maxRestarts);
        }
        long remainingTimeoutMs = timeoutMs;
        for (Context context : batch) {
            try {
                remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, context, State.SERVING, remainingTimeoutMs);
                if (context.currentRoles().broker()) {
                    awaitPreferred(reconciliation, time, rollClient, context, remainingTimeoutMs);
                }
            } catch (TimeoutException e) {
                LOGGER.warnCr(reconciliation, "Timed out waiting for node {} to become ready after a restart", context.nodeRef());
                if (context.numAttempts() >= maxAttempts) {
                    LOGGER.warnCr(reconciliation, "Reached the maximum attempt of waiting for node {} to become ready after a restart", context.nodeRef());
                    throw e;
                } else {
                    context.incrementNumAttempts();
                    return;
                }
            }
        }
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

                if (!loggingDiff.isEmpty()) {
                    return Plan.RECONFIGURE;
                } else {
                    return Plan.NOP;
                }
            } else {
                context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                return Plan.RESTART;
            }
        }));

        return Map.of(
                Plan.RESTART, Stream.concat(byPlan.getOrDefault(Plan.RESTART, List.of()).stream(), refinedPlan.getOrDefault(Plan.RESTART, List.of()).stream()).toList(),
                Plan.RECONFIGURE, refinedPlan.getOrDefault(Plan.RECONFIGURE, List.of()),
                Plan.NOP, Stream.concat(byPlan.getOrDefault(Plan.NOP, List.of()).stream(), refinedPlan.getOrDefault(Plan.NOP, List.of()).stream()).toList()
        );
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, AgentClient agentClient, NodeRef nodeRef) {
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
                    var bs = agentClient.getBrokerState(nodeRef);
                    LOGGER.debugCr(reconciliation, "Node {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() >= BrokerState.RUNNING.value() && bs.value() != BrokerState.UNKNOWN.value()) {
                        state = State.SERVING;
                    } else if (bs.value() == BrokerState.RECOVERY.value()) {
                        LOGGER.warnCr(reconciliation, "Node {} is in log recovery. There are {} logs and {} segments left to recover", nodeRef.nodeId(), bs.remainingLogsToRecover(), bs.remainingSegmentsToRecover());
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
        // Used in {@link #initialPlan(List, RollClient)} for brokers that require reconfigure
        // before we know whether the actual config changes are reconfigurable
        MAYBE_RECONFIGURE,
        // Used in {@link #refinePlanForReconfigurability(Reconciliation, KafkaVersion, Function, String, RollClient, Map)}
        // once we know a MAYBE_RECONFIGURE node can actually be reconfigured
        RECONFIGURE,
        RESTART,
        // Used in {@link #initialPlan(List, RollClient)} for nodes that require waiting for
        // log recovery to complete
        WAIT_FOR_LOG_RECOVERY,
    }

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * @param podOperator               Pod operator for managing pods
     * @param nodes                     The nodes (not all of which may need restarting).
     * @param reconciliation            Reconciliation marker
     * @param predicate                 The predicate used to determine whether to restart a particular node
     * @param clusterCaCertSecret       Secret with the Cluster CA public key
     * @param coKeySecret               Secret with the Cluster CA private key
     * @param adminClientProvider       Kafka Admin client provider
     * @param kafkaAgentClientProvider  Kafka Agent client provider
     * @param kafkaVersion              Kafka version
     * @param allowReconfiguration      Flag indicting whether reconfiguration is allowed or not
     * @param kafkaConfigProvider       Kafka configuration provider
     * @param totalNumOfControllerNodes The total number of controller nodes
     * @param kafkaLogging              Kafka logging configuration
     * @param postOperationTimeoutMs    The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize       The maximum number of nodes that might be restarted at once
     * @param maxRestarts               The maximum number of restart that can be done for a node
     * @param maxReconfigs              The maximum number of reconfiguration that can be done for a node
     * @param maxAttempts               The maximum number to operational attempt that can be done for a node
     * @param eventPublisher            Kubernetes Events publisher for publishing events about node restarts
     * @return RackRolling instance
     */
    public static RackRolling rollingRestart(PodOperator podOperator,
                                             Collection<NodeRef> nodes,
                                             Reconciliation reconciliation,
                                             Function<Integer, RestartReasons> predicate,
                                             TlsPemIdentity coTlsPemIdentity,
                                             AdminClientProvider adminClientProvider,
                                             KafkaAgentClientProvider kafkaAgentClientProvider,
                                             Function<Integer, String> kafkaConfigProvider,
                                             int totalNumOfControllerNodes,
                                             boolean allowReconfiguration,
                                             KafkaVersion kafkaVersion,
                                             String kafkaLogging,
                                             long postOperationTimeoutMs,
                                             int maxRestartBatchSize,
                                             int maxRestarts,
                                             int maxReconfigs,
                                             int maxAttempts,
                                             KubernetesRestartEventPublisher eventPublisher) {
        PlatformClient platformClient = new PlatformClientImpl(podOperator, reconciliation.namespace(), reconciliation, postOperationTimeoutMs, eventPublisher);
        Time time = Time.SYSTEM_TIME;
        final var contextMap = nodes.stream().collect(Collectors.toUnmodifiableMap(NodeRef::nodeId, node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)));

        RollClient rollClient = new RollClientImpl(reconciliation, coTlsPemIdentity, adminClientProvider);
        AgentClient agentClient = new AgentClientImpl(kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity));

        return new RackRolling(time,
                platformClient,
                rollClient,
                agentClient,
                reconciliation,
                totalNumOfControllerNodes,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                kafkaLogging,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                maxAttempts,
                contextMap);
    }

    // visible for testing
    protected static RackRolling rollingRestart(Time time,
                                                PlatformClient platformClient,
                                                RollClient rollClient,
                                                AgentClient agentClient,
                                                Collection<NodeRef> nodes,
                                                Function<Integer, RestartReasons> predicate,
                                                Reconciliation reconciliation,
                                                int totalNumOfControllerNodes,
                                                KafkaVersion kafkaVersion,
                                                boolean allowReconfiguration,
                                                Function<Integer, String> kafkaConfigProvider,
                                                String desiredLogging,
                                                long postOperationTimeoutMs,
                                                int maxRestartBatchSize,
                                                int maxRestarts,
                                                int maxReconfigs,
                                                int maxAttempts) {
        final var contextMap = nodes.stream().collect(Collectors.toUnmodifiableMap(NodeRef::nodeId, node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)));

        return new RackRolling(time,
                platformClient,
                rollClient,
                agentClient,
                reconciliation,
                totalNumOfControllerNodes,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                desiredLogging,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                maxAttempts,
                contextMap);
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final RollClient rollClient;
    private final AgentClient agentClient;
    private final Reconciliation reconciliation;
    /**
     * totalNumOfControllerNodes is used when checking if all controllers are combined and not running
     * and when calculating the majority of the controller for quorum healthCheck.
     **/
    private final int totalNumOfControllerNodes;
    private final KafkaVersion kafkaVersion;
    private final boolean allowReconfiguration;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String desiredLogging;
    private final long postOperationTimeoutMs;
    private final int maxRestartBatchSize;
    private final int maxRestarts;
    private final int maxReconfigs;
    private final int maxAttempts;

    /**
     * Constructor for RackRolling instance
     * @param time                      initial time to set for context
     * @param platformClient            client for platform calls
     * @param rollClient               client for kafka cluster admin calls
     * @param agentClient               client for kafka agent calls
     * @param reconciliation            Reconciliation marker
     * @param totalNumOfControllerNodes Total number of controller nodes in the cluster
     * @param kafkaVersion              Kafka version
     * @param allowReconfiguration      Flag indicting whether reconfiguration is allowed or not
     * @param kafkaConfigProvider       Kafka configuration provider
     * @param desiredLogging            Kafka logging configuration
     * @param postOperationTimeoutMs    The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize       The maximum number of nodes that might be restarted at once
     * @param maxRestarts               The maximum number of restart that can be done for a node
     * @param maxReconfigs              The maximum number of reconfiguration that can be done for a node
     * @param maxAttempts               The maximum number to operational attempt that can be done for a node
     * @param contextMap                Map of contexts for each node
     */
    public RackRolling(Time time,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       AgentClient agentClient,
                       Reconciliation reconciliation,
                       int totalNumOfControllerNodes,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       Function<Integer, String> kafkaConfigProvider,
                       String desiredLogging,
                       long postOperationTimeoutMs,
                       int maxRestartBatchSize,
                       int maxRestarts,
                       int maxReconfigs,
                       int maxAttempts,
                       Map<Integer, Context> contextMap) {
        this.time = time;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.agentClient = agentClient;
        this.reconciliation = reconciliation;
        this.totalNumOfControllerNodes = totalNumOfControllerNodes;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.desiredLogging = desiredLogging;
        this.postOperationTimeoutMs = postOperationTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.maxRestarts = maxRestarts;
        this.maxReconfigs = maxReconfigs;
        this.contextMap = contextMap;
        this.maxAttempts = maxAttempts;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**  Runs the roller via single thread Executor
     *
     * @param vertx Vertx instance
     * @return a future based on the rolling outcome.
     */
    public Future<Void> executeRollingAsync(
            Vertx vertx) {

        Promise<Void> result = Promise.promise();
        var singleExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "kafka-roller"));
        try {
            singleExecutor.submit(() -> {
                try {
                    executeRolling();
                    vertx.runOnContext(ig -> result.complete());
                } catch (Exception e) {
                    LOGGER.debugCr(reconciliation, "Something went wrong when trying to do a rolling restart", e);
                    vertx.runOnContext(ig -> result.fail(e));
                }
            });
        } finally {
            singleExecutor.shutdown();
        }
        return result.future();
    }

    private void executeRolling() throws TimeoutException, InterruptedException, ExecutionException {
        List<Integer> nodesToRestart;
        do {
            nodesToRestart = loop();
        } while (!nodesToRestart.isEmpty());
    }

    /**
     * Process each context to determine which nodes need restarting.
     * Nodes that are not ready (in the Kubernetes sense) will always be considered for restart before any others.
     * The given {@code predicate} will be called for each of the remaining nodes and those for which the function returns a non-empty
     * list of reasons will be restarted.
     *
     * The expected worst case execution time of this function is approximately
     * {@code (timeoutMs * maxRestarts + postOperationTimeoutMs) * size(nodes)}.
     * This is reached when:
     * <ol>
     *     <li>We initially attempt to reconfigure the nodes that have configuration changes</li>
     *     <li>If reconfigurations fail after {@code maxReconfigs}, so we resort to restarts</li>
     *     <li>We require {@code maxRestarts} restarts for each node, and each restart uses the
     *         maximum {@code timeoutMs}.</li>
     * </ol>
     *
     * If a node is not restarted by this method (because the {@code predicate} function returned empty), then
     * it may not be the leader of all its preferred replicas.
     * This method is executed repeatedly until there is no nodes left to restart or reconfigure
     * If this method completes normally then all initially unready nodes and the nodes for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no nodes) will have been successfully restarted and
     * nodes that have configurations changed will have been reconfigured.
     * In other words, successful return from this method indicates that all nodes seem to be up and
     * "functioning normally".
     * If a node fails to restart or recover its logs within a certain time this method will throw TimeoutException.
     *
     * @return list of nodes to retry
     * @throws TimeoutException when waiting for node to become ready after restart
     * @throws InterruptedException UncheckedInterruptionException
     * @throws ExecutionException UncheckedExecutionException
     **/
    public List<Integer> loop() throws TimeoutException, InterruptedException, ExecutionException {
        try {
            // Observe current state and update the contexts
            var contexts = contextMap.values().stream().toList();
            for (var context : contexts) {
                context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            }

            var unreadyNodes = contexts.stream().filter(context -> context.state().equals(State.NOT_READY)).collect(Collectors.toList());
            if (!unreadyNodes.isEmpty()) {
                // We want to give nodes chance to get ready before we try to connect to the or consider them for rolling.
                // This is important especially for nodes which were just started.
                LOGGER.debugCr(reconciliation, "Waiting for nodes {} to become ready before initialising plan in case they just started", unreadyNodes);
                waitForUnreadyNodes(unreadyNodes, true);
            }

            var byPlan = initialPlan(contexts, rollClient);
            LOGGER.debugCr(reconciliation, "Initial plan: {}", byPlan);

            if (!byPlan.getOrDefault(Plan.WAIT_FOR_LOG_RECOVERY, List.of()).isEmpty()) {
                return waitForLogRecovery(byPlan.get(Plan.WAIT_FOR_LOG_RECOVERY));
            }

            // Restart any initially unready nodes
            if (!byPlan.getOrDefault(Plan.RESTART_FIRST, List.of()).isEmpty()) {
                return restartUnReadyNodes(byPlan.get(Plan.RESTART_FIRST), totalNumOfControllerNodes);
            }

            // If we get this far we know all nodes are ready
            LOGGER.debugCr(reconciliation, "Initialising the admin client for controller nodes");
            rollClient.initialiseControllerAdmin();
            LOGGER.debugCr(reconciliation, "Initialising the admin client for broker nodes");
            rollClient.initialiseBrokerAdmin();

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
            if (!byPlan.getOrDefault(Plan.RECONFIGURE, List.of()).isEmpty()) {
                return reconfigureNodes(byPlan.get(Plan.RECONFIGURE));
            }

            if (byPlan.getOrDefault(Plan.RESTART, List.of()).isEmpty()) {
                unreadyNodes = contexts.stream().filter(context -> context.state().equals(State.NOT_READY)).collect(Collectors.toList());
                if (unreadyNodes.isEmpty()) {
                    LOGGER.debugCr(reconciliation, "Terminate: No Kafka nodes left to reconcile");
                    return List.of();
                } else {
                    // By checking nodes which don't need to be restarted for readiness we prevent successive reconciliations
                    // from taking out a node each time (due, e.g. to a configuration error).
                    LOGGER.debugCr(reconciliation, "Nodes {} do not need to be restarted", unreadyNodes);
                    LOGGER.debugCr(reconciliation, "Waiting for non-restarted nodes {} to become ready", unreadyNodes);
                    return waitForUnreadyNodes(unreadyNodes, false);
                }
            }

            // If we get this far then all remaining nodes require a restart
            var restartedNodes = restartNodes(byPlan.get(Plan.RESTART), totalNumOfControllerNodes);
            if (contexts.stream().allMatch(context -> context.state().equals(State.LEADING_ALL_PREFERRED))) {
                LOGGER.debugCr(reconciliation, "Reconciliation completed successfully: All nodes leading preferred replicas after restart");
                return List.of();
            }
            return restartedNodes;

        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (UncheckedExecutionException e) {
            throw e.getCause();
        } finally {
            try {
                rollClient.closeControllerAdminClient();
            } catch (RuntimeException e) {
                LOGGER.debugCr(reconciliation, "Exception closing controller admin client", e);
            }

            try {
                rollClient.closeBrokerAdminClient();
            } catch (RuntimeException e) {
                LOGGER.debugCr(reconciliation, "Exception closing broker admin client", e);
            }
        }
    }

    private List<Integer> restartNodes(List<Context> nodesToRestart, int totalNumOfControllerNodes) throws TimeoutException {
        // determine batches of nodes to be restarted together
        var batch = nextBatch(reconciliation, rollClient, contextMap, nodesToRestart.stream().collect(Collectors.toMap(
                Context::nodeId,
                Context::currentRoles
        )), totalNumOfControllerNodes, maxRestartBatchSize);

        if (batch.isEmpty()) {
            nodesToRestart.forEach(c -> {
                // check if the maxAttempt for this node has reached
                if (c.numAttempts() >= maxAttempts) {
                    throw new UnrestartableNodesException("Cannot restart nodes " + nodesToRestart.stream().map(Context::nodeRef).toList() +
                            " because some of them violating quorum health or some topics' min.in.sync.replicas. " +
                            "The max attempts (" + maxAttempts + ") to retry the nodes has been reached.");
                }
                c.incrementNumAttempts();
            });
            time.sleep(postOperationTimeoutMs, 0);
            return nodesToRestart.stream().map(Context::nodeId).collect(Collectors.toList());
        }

        var batchOfIds = batch.stream().map(KafkaNode::id).collect(Collectors.toSet());
        var batchOfContexts = nodesToRestart.stream().filter(context -> batchOfIds.contains(context.nodeId())).collect(Collectors.toSet());
        LOGGER.debugCr(reconciliation, "Restart batch: {}", batchOfContexts);
        // restart a batch
        restartInParallel(reconciliation, time, platformClient, rollClient, agentClient, batchOfContexts, postOperationTimeoutMs, maxRestarts);

        return batchOfIds.stream().toList();
    }

    private List<Integer> reconfigureNodes(List<Context> contexts) {
        List<Integer> reconfiguredNode = List.of();
        for (var context : contexts) {
            // TODO decide whether to support canary reconfiguration for cluster-scoped configs (nice to have)
            try {
                reconfigureNode(reconciliation, time, rollClient, context, maxReconfigs);
            } catch (RuntimeException e) {
                return List.of(context.nodeId());
            }

            time.sleep(postOperationTimeoutMs / 2, 0);
            // TODO decide whether we need an explicit healthcheck here
            //      or at least to know that the kube health check probe will have failed at the time
            //      we break to OUTER (We need to test a scenario of breaking configuration change, does this sleep catch it?)
            awaitPreferred(reconciliation, time, rollClient, context, postOperationTimeoutMs / 2);
            // termination condition
            if (contexts.stream().allMatch(context2 -> context2.state().equals(State.LEADING_ALL_PREFERRED))) {
                LOGGER.debugCr(reconciliation, "Terminate: All nodes leading preferred replicas after reconfigure");
                break;
            }
            reconfiguredNode = List.of(context.nodeId());
        }
        return reconfiguredNode;
    }

    private List<Integer> waitForLogRecovery(List<Context> contexts) {
        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context c : contexts) {
            try {
                remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, c, State.SERVING, remainingTimeoutMs);
            } catch (TimeoutException e) {
                if (c.numAttempts() >= maxAttempts) {
                    var brokerState = agentClient.getBrokerState(c.nodeRef());
                    throw new UnrestartableNodesException("The max attempts (" + maxAttempts + ") to wait for this node "  +  c.nodeRef() + " to finish performing log recovery has been reached. " +
                            "There are " + brokerState.remainingLogsToRecover() + " logs and " + brokerState.remainingSegmentsToRecover() + " segments left to recover.");
                }
                c.incrementNumAttempts();
            }
        }
        return contexts.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    private List<Integer> waitForUnreadyNodes(List<Context> contexts, boolean ignoreTimeout) {
        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context context : contexts) {
            try {
                remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, context, State.SERVING, remainingTimeoutMs);
            } catch (TimeoutException e) {
                if (ignoreTimeout) {
                    return List.of();
                } else if (context.numAttempts() >= maxAttempts) {
                    throw new RuntimeException(e);
                } else {
                    context.incrementNumAttempts();
                }
            }
        }
        return contexts.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    private List<Integer> restartUnReadyNodes(List<Context> contexts, int totalNumOfControllers) throws TimeoutException {
        Set<Context> pureControllerNodesToRestart = new HashSet<>();
        Set<Context> combinedNodesToRestart = new HashSet<>();
        var notRunningCombinedNodes = 0;

        for (var context : contexts) {
            if (context.currentRoles().controller() && !context.currentRoles().broker()) {
                pureControllerNodesToRestart.add(context);
            } else if (context.currentRoles().controller() && context.currentRoles().broker()) {
                combinedNodesToRestart.add(context);
                if (context.state().equals(State.NOT_RUNNING)) {
                    notRunningCombinedNodes++;
                }
            }
        }

        if (totalNumOfControllers > 1 && totalNumOfControllers == notRunningCombinedNodes) {
            LOGGER.warnCr(reconciliation, "All controller nodes are combined and they are not running, therefore restarting them all now");
            // if all controller nodes (except a single node quorum) are combined and all of them are not running e.g. Pending, we need to restart them all at the same time to form the quorum.
            // This is because until the quorum has been formed and broker process can connect to it, the combined nodes do not become ready.
            restartInParallel(reconciliation, time, platformClient, rollClient, agentClient, combinedNodesToRestart, postOperationTimeoutMs, maxRestarts);
            return combinedNodesToRestart.stream().map(Context::nodeId).toList();
        }

        // restart in the following order: pure controllers, combined nodes and brokers
        Context nodeToRestart;
        if (pureControllerNodesToRestart.size() > 0) {
            nodeToRestart = pureControllerNodesToRestart.iterator().next();
        } else if (combinedNodesToRestart.size() > 0) {
            nodeToRestart = combinedNodesToRestart.iterator().next();
        } else {
            nodeToRestart = contexts.get(0);
        }

        if (nodeToRestart.state() == State.NOT_RUNNING && !nodeToRestart.reason().contains(RestartReason.POD_HAS_OLD_REVISION)) {
            // If the node is not running (e.g. unschedulable) then restarting it, likely won't make any difference.
            // Proceeding and deleting another node may result in it not running too. Avoid restarting it unless it has an old revision.
            LOGGER.warnCr(reconciliation, "Node {} has been already restarted but still not running. Therefore will not restart it", nodeToRestart);
        } else {
            restartNode(reconciliation, time, platformClient, nodeToRestart, maxRestarts);
        }

        try {
            long remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, nodeToRestart, State.SERVING, postOperationTimeoutMs);
            if (nodeToRestart.currentRoles().broker()) {
                awaitPreferred(reconciliation, time, rollClient, nodeToRestart, remainingTimeoutMs);
            }
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for node {} to become ready after a restart", nodeToRestart.nodeRef());
            if (nodeToRestart.numAttempts() >= maxAttempts) {
                LOGGER.warnCr(reconciliation, "Reached the maximum attempt of waiting for node {} to become ready after a restart", nodeToRestart.nodeRef());
                throw e;
            }
            nodeToRestart.incrementNumAttempts();
        }
        return List.of(nodeToRestart.nodeId());
    }

    private Map<Plan, List<Context>> initialPlan(List<Context> contexts, RollClient rollClient) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_RUNNING) {
                LOGGER.debugCr(reconciliation, "{} is in {} state therefore may get restarted first", context.nodeRef(), context.state());
                context.reason().add(RestartReason.POD_STUCK);
                return Plan.RESTART_FIRST;

            } else if (context.state() == State.RECOVERING) {
                LOGGER.debugCr(reconciliation, "{} is in log recovery therefore will not be restarted", context.nodeRef());
                return Plan.WAIT_FOR_LOG_RECOVERY;

            } else if (!rollClient.canConnectToNode(context.nodeRef(), context.currentRoles().controller())) {
                LOGGER.debugCr(reconciliation, "{} will be restarted because it does not seem to responding to connection attempt", context.nodeRef());
                context.reason().add(RestartReason.POD_UNRESPONSIVE);
                return Plan.RESTART_FIRST;

            } else {
                var reasons = context.reason();
                if (!reasons.getReasons().isEmpty()) {

                    if (context.numRestarts() > 0
                            && (context.state() == State.LEADING_ALL_PREFERRED
                            || context.state() == State.SERVING)) {
                        LOGGER.debugCr(reconciliation, "{} has been already restarted and is in {} state therefore will not be restarted", context.nodeRef(), context.state());
                        return Plan.NOP;
                    } else {
                        LOGGER.debugCr(reconciliation, "{} will be restarted because of {}", context.nodeRef(), reasons);
                        return Plan.RESTART;
                    }
                } else if (context.currentRoles().controller() && !context.currentRoles().broker() && context.state() == State.SERVING) {
                    // The roller does not attempt to reconfigure pure controller.
                    // If a pure controller's configuration has changed, it should have non-empty reasons to restart.
                    return Plan.NOP;
                } else {
                    if (context.numReconfigs() > 0
                            && context.state() == State.LEADING_ALL_PREFERRED) {
                        LOGGER.debugCr(reconciliation, "{} has already been reconfigured", context.nodeRef());
                        return Plan.NOP;
                    } else {
                        LOGGER.debugCr(reconciliation, "{} may need to be reconfigured", context.nodeRef());
                        return Plan.MAYBE_RECONFIGURE;
                    }
                }
            }
        }));
    }

}
