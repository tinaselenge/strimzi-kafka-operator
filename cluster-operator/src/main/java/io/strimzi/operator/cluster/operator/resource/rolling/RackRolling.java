/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RackRolling
 */
@SuppressWarnings({"ParameterNumber" })
public class RackRolling {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;
    private final List<Context> contexts;

    enum Action {
        // Used for brokers that are initially healthy and require neither restart no reconfigure
        NOP,
        // Used in for nodes that are not healthy and require neither restart
        WAIT_FOR_NON_RESTARTED,
        WAIT_FOR_LOG_RECOVERY,
        // Used in {@link #initialPlan(List, RollClient)} for nodes that require reconfigure
        // before we know whether the actual config changes are reconfigurable
        MAYBE_RECONFIGURE,
        // Used in {@link #refinePlanForReconfigurability(Reconciliation, KafkaVersion, Function, String, RollClient, Map)}
        // once we know a MAYBE_RECONFIGURE node can actually be reconfigured
        RECONFIGURE,
        RESTART_UNHEALTHY,
        RESTART,
    }

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * @param reconciliation           Reconciliation marker
     * @param podOperator              Pod operator for managing pods
     * @param pollingIntervalMs        The maximum number of restart that can be done for a node
     * @param postOperationTimeoutMs   The maximum time in milliseconds to wait after a restart or reconfigure
     * @param backOffSupplier          Backoff supplier
     * @param nodes                    The nodes (not all of which may need restarting).
     * @param coTlsPemIdentity         Cluster operator PEM identity
     * @param adminClientProvider      Kafka Admin client provider
     * @param kafkaAgentClientProvider Kafka Agent client provider
     * @param predicate                The predicate used to determine whether to restart a particular node
     * @param kafkaConfigProvider      Kafka configuration provider
     * @param kafkaVersion             Kafka version
     * @param allowReconfiguration     Flag indicting whether reconfiguration is allowed or not
     * @param maxReconfigs             The maximum number of reconfiguration that can be done for a node
     * @param maxRestartBatchSize      The maximum number of nodes that might be restarted at once
     * @param eventPublisher           Kubernetes Events publisher for publishing events about node restarts
     * @return RackRolling instance
     */
    public static RackRolling initialise(Reconciliation reconciliation,
                                         PodOperator podOperator,
                                         long pollingIntervalMs,
                                         long postOperationTimeoutMs,
                                         Supplier<BackOff> backOffSupplier,
                                         Collection<NodeRef> nodes,
                                         TlsPemIdentity coTlsPemIdentity,
                                         AdminClientProvider adminClientProvider,
                                         KafkaAgentClientProvider kafkaAgentClientProvider,
                                         Function<Pod, RestartReasons> predicate,
                                         Function<Integer, String> kafkaConfigProvider,
                                         KafkaVersion kafkaVersion,
                                         boolean allowReconfiguration,
                                         int maxReconfigs,
                                         int maxRestartBatchSize,
                                         KubernetesRestartEventPublisher eventPublisher) {
        PlatformClient platformClient = new PlatformClientImpl(podOperator, reconciliation.namespace(), reconciliation, postOperationTimeoutMs, eventPublisher);
        Time time = Time.SYSTEM_TIME;
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        RollClient rollClient = new RollClientImpl(reconciliation, coTlsPemIdentity, adminClientProvider);
        AgentClient agentClient = new AgentClientImpl(kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity));

        return new RackRolling(time,
                reconciliation,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxReconfigs,
                contexts,
                platformClient,
                rollClient,
                agentClient,
                kafkaConfigProvider, kafkaVersion,
                allowReconfiguration
        );
    }

    // visible for testing
    protected static RackRolling initialise(Time time,
                                            PlatformClient platformClient,
                                            RollClient rollClient,
                                            AgentClient agentClient,
                                            Collection<NodeRef> nodes,
                                            PodOperator podOperator,
                                            Function<Pod, RestartReasons> predicate,
                                            Reconciliation reconciliation,
                                            KafkaVersion kafkaVersion,
                                            boolean allowReconfiguration,
                                            Function<Integer, String> kafkaConfigProvider,
                                            long postOperationTimeoutMs,
                                            long pollingIntervalMs,
                                            int maxRestartBatchSize,
                                            int maxReconfigs,
                                            Supplier<BackOff> backOffSupplier) {
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        return new RackRolling(time,
                reconciliation,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxReconfigs,
                contexts,
                platformClient,
                rollClient,
                agentClient,
                kafkaConfigProvider,
                kafkaVersion,
                allowReconfiguration
        );
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final RollClient rollClient;
    private final AgentClient agentClient;
    private final Reconciliation reconciliation;
    private final KafkaVersion kafkaVersion;
    private final boolean allowReconfiguration;
    private final Function<Integer, String> kafkaConfigProvider;
    private final long postOperationTimeoutMs;
    private final int maxRestartBatchSize;
    private final long pollingIntervalMs;
    private final int maxReconfigs;

    /**
     * Constructor for RackRolling instance
     *
     * @param time                   initial time to set for context
     * @param reconciliation         Reconciliation marker
     * @param pollingIntervalMs      The maximum number of restart that can be done for a node
     * @param postOperationTimeoutMs The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize    The maximum number of nodes that might be restarted at once
     * @param maxReconfigs           The maximum number of reconfiguration that can be done for a node
     * @param contexts               List of context for each node
     * @param platformClient         client for platform calls
     * @param rollClient             client for kafka cluster admin calls
     * @param agentClient            client for kafka agent calls
     * @param kafkaConfigProvider    Kafka configuration provider
     * @param kafkaVersion           Kafka version
     * @param allowReconfiguration   Flag indicting whether reconfiguration is allowed or not
     */
    public RackRolling(Time time,
                       Reconciliation reconciliation,
                       long pollingIntervalMs,
                       long postOperationTimeoutMs,
                       int maxRestartBatchSize,
                       int maxReconfigs, List<Context> contexts,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       AgentClient agentClient,
                       Function<Integer, String> kafkaConfigProvider,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration) {
        this.time = time;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.agentClient = agentClient;
        this.reconciliation = reconciliation;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.postOperationTimeoutMs = postOperationTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.maxReconfigs = maxReconfigs;
        this.contexts = contexts;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**
     * Runs the roller via single thread Executor
     *
     * @param vertx Vertx instance
     * @return a future based on the rolling outcome.
     */
    public Future<Void> rollingRestart(
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

            singleExecutor.shutdown();
        }
        return result.future();
    }

    private void executeRolling() throws InterruptedException, ExecutionException {
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
     * If a broker node is restarted by this method (because the {@code predicate} function returned empty), then
     * it will be elected as a leader of all its preferred replicas if it's not leading them yet.
     * However, if failed to lead them within a certain time, this will result in a warning log and moving onto the next node.
     *
     * This method is executed repeatedly until there is no nodes left to restart or reconfigure or max attempt is reached for any node.
     * If this method completes normally then all initially unready nodes and the nodes for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no nodes) will have been successfully restarted and
     * nodes that have configurations changed will have been reconfigured.
     * In other words, successful return from this method indicates that all nodes seem to be up and
     * "functioning normally".
     * If a node fails to become ready after a restart (e.g. recovering its logs) within a certain time, it will be retried for a restart or wait
     * until the maximum restart or maximum attempt has reached.
     *
     * If the maximum restart reached for any node, this method will throw MaxRestartsExceededException.
     * If the maximum attempt reached for any node, this method will throw MaxAttemptsExceededException.
     * If any node is not running but has an up-to-date revision, this method will throw UnrestartableNodesException.
     * If any node is not ready after a restart or could not be restarted, this method will throw UnrestartableNodesException.
     *
     * @return list of nodes to retry
     * @throws InterruptedException UncheckedInterruptionException  The thread was interrupted
     * @throws ExecutionException UncheckedExecutionException Execution exception from clients
     **/
    public List<Integer> loop() throws InterruptedException, ExecutionException {
        try {
            // Observe current state and update the contexts
            for (var context : contexts) {
                context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            }

            // We want to give nodes chance to get ready before we try to connect to the or consider them for rolling.
            // This is important especially for nodes which were just started.
            waitForNodeReadiness(contexts.stream().filter(context -> context.state().equals(State.NOT_READY)).collect(Collectors.toList()),
                    (c, e) -> LOGGER.debugCr(reconciliation, "Waited for node {} to become ready before initialising plan in case they just started", c));

            var byPlan = initialPlan(contexts, rollClient);
            LOGGER.debugCr(reconciliation, "Initial plan: {}", byPlan.entrySet().stream().map(plan -> String.format("%s=%s %n", plan.getKey(), plan.getValue())).collect(Collectors.toSet()));

            if (!byPlan.getOrDefault(Action.WAIT_FOR_LOG_RECOVERY, List.of()).isEmpty()) {
                return waitForLogRecovery(byPlan.get(Action.WAIT_FOR_LOG_RECOVERY));
            }

            // Restart any initially unready nodes
            if (!byPlan.getOrDefault(Action.RESTART_UNHEALTHY, List.of()).isEmpty()) {
                return restartUnhealthyNodes(byPlan.get(Action.RESTART_UNHEALTHY));
            }

            if (!byPlan.getOrDefault(Action.WAIT_FOR_NON_RESTARTED, List.of()).isEmpty()) {
                waitForNonRestartedNodeReadiness(byPlan.get(Action.WAIT_FOR_NON_RESTARTED));
            }

            rollClient.initialiseControllerAdmin(contexts.stream().filter(c -> c.currentRoles().controller()).map(Context::nodeRef).collect(Collectors.toSet()));
            rollClient.initialiseBrokerAdmin(contexts.stream().filter(c -> c.currentRoles().broker()).map(Context::nodeRef).collect(Collectors.toSet()));

            // Refine the plan, reassigning nodes under MAYBE_RECONFIGURE to either RECONFIGURE or RESTART
            // based on whether they have only reconfiguration config changes
            List<Context> maybeConfigureNodes = byPlan.getOrDefault(Action.MAYBE_RECONFIGURE, List.of());
            if (allowReconfiguration && !maybeConfigureNodes.isEmpty()) {
                var nodeConfigs = getNodeConfigs(rollClient, maybeConfigureNodes);

                byPlan = refinePlanForReconfigurability(reconciliation,
                        kafkaVersion,
                        kafkaConfigProvider,
                        maybeConfigureNodes,
                        nodeConfigs,
                        byPlan);
                LOGGER.debugCr(reconciliation, "Refined plan: {}", byPlan.entrySet().stream().map(plan -> String.format("%s=%s %n", plan.getKey(), plan.getValue())).collect(Collectors.toSet()));
            }

            // Reconfigure any reconfigurable nodes
            if (!byPlan.getOrDefault(Action.RECONFIGURE, List.of()).isEmpty()) {
                return reconfigureNodes(byPlan.get(Action.RECONFIGURE));
            }

            // If we get this far then all remaining nodes require a restart
            if (!byPlan.getOrDefault(Action.RESTART, List.of()).isEmpty()) {
                return restartNodes(byPlan.get(Action.RESTART));
            }

            if (byPlan.getOrDefault(Action.NOP, List.of()).size() == contexts.size()) {
                LOGGER.debugCr(reconciliation, "Reconciliation completed successfully: All nodes are ready after restart");
                return List.of();
            }

            return contexts.stream()
                    .filter(c -> !c.state().equals(State.READY))
                    .map(c -> c.nodeRef().nodeId())
                    .collect(Collectors.toList());
        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (UncheckedExecutionException e) {
            throw e.getCause();
        }
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
                state = State.READY;
                break;
            case NOT_READY:
            default:
                try {
                    var bs = agentClient.getBrokerState(nodeRef);
                    LOGGER.debugCr(reconciliation, "Node {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() >= BrokerState.RUNNING.value() && bs.value() != BrokerState.UNKNOWN.value()) {
                        state = State.READY;
                    } else if (bs.value() == BrokerState.RECOVERY.value()) {
                        LOGGER.warnCr(reconciliation, "Node {} is in log recovery. There are {} logs and {} segments left to recover", nodeRef.nodeId(), bs.remainingLogsToRecover(), bs.remainingSegmentsToRecover());
                        state = State.RECOVERING;
                    } else {
                        state = State.NOT_READY;
                    }
                } catch (Exception e) {
                    //TODO: since observe method is called very frequently, maybe we should not print the full error cause in each message, maybe just in debug?
                    LOGGER.warnCr(reconciliation, "Could not get broker state for node {}. This might be temporary if a node was just restarted", nodeRef, e);
                    state = State.NOT_READY;
                }
        }
        LOGGER.debugCr(reconciliation, "Node {}: observation outcome is {}", nodeRef, state);
        return state;
    }

    private List<Integer> waitForNodeReadiness(List<Context> contexts, BiConsumer<Context, TimeoutException> timeoutHandler) {
        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context context : contexts) {
            try {
                remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
            } catch (TimeoutException e) {
                timeoutHandler.accept(context, e);
            }
        }
        return contexts.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    private long awaitReadyState(Context context, long timeoutMs) throws TimeoutException {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to enter state {}", context, State.READY);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + State.READY + " within " + timeoutMs + " ms: " + context
        ).poll(pollingIntervalMs, () -> {
            var state = context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            return state == State.READY;
        });
    }

    private Map<Action, List<Context>> initialPlan(List<Context> contexts, RollClient rollClient) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_RUNNING) {
                LOGGER.debugCr(reconciliation, "{} is in {} state therefore may get restarted first", context.nodeRef(), context.state());
                return Action.RESTART_UNHEALTHY;

            } else if (context.state() == State.RECOVERING) {
                LOGGER.debugCr(reconciliation, "{} is in log recovery therefore will not be restarted", context.nodeRef());
                return Action.WAIT_FOR_LOG_RECOVERY;

            } else if (!rollClient.canConnectToNode(context.nodeRef(), context.currentRoles().broker())) {
                LOGGER.debugCr(reconciliation, "{} will be restarted because it does not seem to responding to connection attempt", context.nodeRef());
                context.reason().add(RestartReason.POD_UNRESPONSIVE);
                return Action.RESTART_UNHEALTHY;

            } else {
                if (context.reason().getReasons().isEmpty()) {
                    // Node might have been restarted in a previous reconciliation and left in unready state
                    return context.state().equals(State.READY) ? Action.MAYBE_RECONFIGURE : Action.WAIT_FOR_NON_RESTARTED;
                }

                if (context.numRestarts() > 0 && context.state().equals(State.READY)) {
                    return Action.NOP;
                }

                return Action.RESTART;
            }
        }));
    }

    private List<Integer> waitForLogRecovery(List<Context> contexts) {
        List<Long> delay = new ArrayList<>();
        List<Integer> nodesInRecovery = waitForNodeReadiness(contexts, (c, e) -> {
            var brokerState = agentClient.getBrokerState(c.nodeRef());
            if (c.backOff().done()) {
                throw new MaxAttemptsExceededException("The max attempts (" + c.backOff().maxAttempts() + ") to wait for this node "  +  c.nodeRef() + " to finish performing log recovery has been reached. " +
                        "There are " + brokerState.remainingLogsToRecover() + " logs and " + brokerState.remainingSegmentsToRecover() + " segments left to recover.");
            }
            delay.add(c.backOff().delayMs());
        });
        Collections.sort(delay);
        time.sleep(delay.get(delay.size() - 1), 0);
        return nodesInRecovery;
    }

    private List<Integer> restartUnhealthyNodes(List<Context> contexts) {
        Set<Context> notRunningControllers = new HashSet<>();
        Set<Context> pureControllerNodesToRestart = new HashSet<>();
        Set<Context> combinedNodesToRestart = new HashSet<>();

        for (var c : contexts) {
            if (c.state() == State.NOT_RUNNING) {
                if (!c.reason().contains(RestartReason.POD_HAS_OLD_REVISION)) {
                    // If the node is not running (e.g. unschedulable) then restarting it, likely won't make any difference.
                    // Proceeding and deleting another node may result in it not running too. Avoid restarting it unless it has an old revision.
                    throw new UnrestartableNodesException("Pod " + c.nodeRef().podName() + " is unschedulable or is not starting");
                }

                // Collect all not running controllers to restart them in parallel
                if (c.currentRoles().controller()) {
                    notRunningControllers.add(c);
                    continue;
                }
            }

            if (c.currentRoles().controller()) {
                if (c.currentRoles().broker()) {
                    combinedNodesToRestart.add(c);
                } else {
                    // we always restart a pure controller first, so if we have any, we exit the loop here
                    pureControllerNodesToRestart.add(c);
                    break;
                }
            }
        }

        if (!notRunningControllers.isEmpty()) {
            LOGGER.debugCr(reconciliation, "There are multiple controllers {} that are not running, which runs a risk of losing the quorum. Restarting them in parallel", notRunningControllers);
            restartInParallel(notRunningControllers);
            return notRunningControllers.stream().map(Context::nodeId).collect(Collectors.toList());
        }

        // restart in the following order: pure controllers, combined nodes and brokers
        Context nodeToRestart = !pureControllerNodesToRestart.isEmpty() ? pureControllerNodesToRestart.iterator().next()
                : !combinedNodesToRestart.isEmpty() ? combinedNodesToRestart.iterator().next()
                : contexts.get(0);

        restartInParallel(Collections.singleton(nodeToRestart));
        return Collections.singletonList(nodeToRestart.nodeId());
    }

    private void restartInParallel(Set<Context> batch) {
        for (Context context : batch) {
            restartNode(context);
        }
        //TODO: Apply post restart delay here
        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context context : batch) {
            try {
                remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
                if (context.currentRoles().broker()) {
                    awaitPreferred(context, remainingTimeoutMs);
                }
            } catch (TimeoutException e) {
                throw new UnrestartableNodesException("Timed out waiting for restarted pod " + context.nodeRef().podName() + " to become ready", e);
            }
        }
    }

    private void restartNode(Context context) {
        LOGGER.debugCr(reconciliation, "Node {}: Restarting", context.nodeRef());
        try {
            platformClient.restartNode(context.nodeRef(), context.reason());
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "An exception thrown during the restart of the node {}", context.nodeRef(), e);
        }
        context.incrementNumRestarts();
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Node {}: Restarted", context.nodeRef());
    }

    private void awaitPreferred(Context context, long timeoutMs) {
        // TODO: apply configured delay (via env variable) before triggering leader election.
        //  This should be probably passed to tryElectAllPreferredLeaders so that delay is only applied
        //  if there are topic partitions to elect, otherwise no point of delaying the process
        time.sleep(10000L, 0);
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to be leader of all its preferred replicas", context);
        try {
            Alarm.timer(time,
                            timeoutMs,
                            () -> "Failed to elect the preferred leader " + context + " for topic partitions within " + timeoutMs)
                    .poll(pollingIntervalMs, () -> rollClient.tryElectAllPreferredLeaders(context.nodeRef()) == 0);
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for node to be leader for all its preferred replicas");
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Failed to elect preferred replica", e);
        }
    }
    private void waitForNonRestartedNodeReadiness(List<Context> unreadyNodes) {
        waitForNodeReadiness(unreadyNodes, (c, e) -> {
            throw new UnrestartableNodesException("Timed out waiting for non-restarted pod " + c.nodeRef().podName() + " to become ready", e);
        });
    }

    private Map<Integer, Config> getNodeConfigs(RollClient rollClient, List<Context> contexts) {
        Map<Integer, Config> nodeConfigs = new HashMap<>();
        try {
            nodeConfigs.putAll(rollClient.describeBrokerConfigs(contexts.stream().filter(c -> c.currentRoles().broker())
                    .map(Context::nodeRef).toList()));
            nodeConfigs.putAll(rollClient.describeControllerConfigs(contexts.stream().filter(c -> c.currentRoles().controller() && !c.currentRoles().broker())
                    .map(Context::nodeRef).toList()));
        } catch (Exception e) {
            // Fail the reconciliation if we cannot get configurations for nodes.
            // This will trigger a new reconciliation which will check Admin client connections to the nodes and restart them if needed.
            // TODO: Can there be situations where Admin client connection works for all nodes but the describe requests keep failing? Could we end up in a failed reconciliation loop?
            //  Do we increment the attempt for all of them?
            //  Possible exceptions: InvalidRequestException, ClusterAuthorizationException, UnsupportedVersionException, TimeoutException, UnknownBrokerExceptions/ConfigResourceNotFoundException
            //  I don't think these exceptions should trigger node restart anyway, but likely need to be fixed outside of the reconciliation, therefore makes sense to fail the reconciliation at this point.
            //  Currently the roller describes one broker at a time. Then marks the broker to restart immediately if the request failed. Is that we should do? Or should retrieve which broker it failed for?
            LOGGER.errorCr(reconciliation, "Error getting configs for : " + contexts, e.getCause());
            throw e;
        }

        return nodeConfigs;
    }

    private static Map<Action, List<Context>> refinePlanForReconfigurability(Reconciliation reconciliation,
                                                                             KafkaVersion kafkaVersion,
                                                                             Function<Integer, String> kafkaConfigProvider,
                                                                             List<Context> contexts,
                                                                             Map<Integer, Config> nodeConfigs,
                                                                             Map<Action, List<Context>> byPlan) {
        var refinedPlan = contexts.stream().collect(Collectors.groupingBy(context -> {
            Config configs = nodeConfigs.get(context.nodeId());

            var diff = new KafkaConfigurationDiff(reconciliation,
                    configs,
                    kafkaConfigProvider.apply(context.nodeId()),
                    kafkaVersion,
                    context.nodeRef(),
                    context.currentRoles().controller(),
                    context.currentRoles().broker());
            context.configDiff(diff);

            if (!diff.isEmpty()) {
                if (diff.canBeUpdatedDynamically()) {
                    return Action.RECONFIGURE;
                } else {
                    context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                    return Action.RESTART;
                }
            }
            return Action.NOP;
        }));

        return Map.of(
                Action.RESTART, Stream.concat(byPlan.getOrDefault(Action.RESTART, List.of()).stream(), refinedPlan.getOrDefault(Action.RESTART, List.of()).stream()).toList(),
                Action.RECONFIGURE, refinedPlan.getOrDefault(Action.RECONFIGURE, List.of()),
                Action.NOP, Stream.concat(byPlan.getOrDefault(Action.NOP, List.of()).stream(), refinedPlan.getOrDefault(Action.NOP, List.of()).stream()).toList()
        );
    }

    private List<Integer> reconfigureNodes(List<Context> contexts) {
        List<Integer> reconfiguredNode = new ArrayList<>();
        for (var context : contexts) {
            if (context.numReconfigs() >= maxReconfigs) {
                LOGGER.warnCr(reconciliation, "The maximum number of configuration attempt reached for node {}, will be restarted.", context.nodeRef());
                context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                return List.of(context.nodeId());
            }

            try {
                reconfigureNode(reconciliation, time, rollClient, context);
            } catch (Exception e) {
                LOGGER.warnCr(reconciliation, "Failed to reconfigure {} due to {}", context.nodeRef(), e);
                context.incrementNumReconfigs();
                return List.of(context.nodeId());
            }

            time.sleep(postOperationTimeoutMs / 2, 0);
            waitForNonRestartedNodeReadiness(Collections.singletonList(context));
            // TODO decide whether we need an explicit healthcheck here
            //      or at least to know that the kube health check probe will have failed at the time
            //      we break to OUTER (We need to test a scenario of breaking configuration change, does this sleep catch it?)
            reconfiguredNode.add(context.nodeId());
            break;
        }
        return reconfiguredNode;
    }

    private static void reconfigureNode(Reconciliation reconciliation,
                                        Time time,
                                        RollClient rollClient,
                                        Context context) {
        LOGGER.debugCr(reconciliation, "Node {}: Reconfiguring", context.nodeRef());
        rollClient.reconfigureNode(context.nodeRef(), context.configDiff(), context.currentRoles().broker());
        context.incrementNumReconfigs();
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Node {}: Reconfigured", context.nodeRef());
    }

    private List<Integer> restartNodes(List<Context> nodesToRestart) {
        // determine batches of nodes to be restarted together
        // for controller nodes, a batch with a single node will be returned
        var batch = nextBatch(reconciliation, rollClient, nodesToRestart, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Restart batch: {}", batch);

        // Empty batch means, there is no node that can safely restarted without violating quorum health or availability.
        if (batch.isEmpty()) {
            List<Long> delay = new ArrayList<>();

            // check if the maxAttempt for any of the nodes has reached
            nodesToRestart.forEach(c -> {
                if (c.backOff().done()) {
                    throw new MaxAttemptsExceededException("Cannot restart nodes " + nodesToRestart.stream().map(Context::nodeRef).toList() +
                            " because they violate quorum health or topic availability. " +
                            "The max attempts (" + c.backOff().maxAttempts() + ") to retry the nodes has been reached.");
                }
                delay.add(c.backOff().delayMs());
            });
            Collections.sort(delay);
            // sleep and retry the nodes
            time.sleep(delay.get(delay.size() - 1), 0);

            return nodesToRestart.stream().map(Context::nodeId).collect(Collectors.toList());
        }

        // restart a batch
        restartInParallel(batch);
        return batch.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    /**
     * Figures out a batch of nodes that can be restarted together.
     * This method enforces the following roll order:
     * <ol>
     *     <li>Pure controller</li>
     *     <li>Combined node</li>
     *     <li>Active controller</li>
     *     <li>Broker (only this case is parallelizable)</li>
     * </ol>
     *
     * @param rollClient The roll client
     * @param nodesToRestart The ids of the nodes which need to be restarted
     * @param maxRestartBatchSize The maximum allowed size for a batch
     * @return The nodes corresponding to a subset of {@code nodeIdsNeedingRestart} that can safely be rolled together
     */
    private Set<Context> nextBatch(Reconciliation reconciliation,
                                   RollClient rollClient,
                                   List<Context> nodesToRestart,
                                   int maxRestartBatchSize) {

        var controllersToRestart = nodesToRestart.stream().filter(c -> c.currentRoles().controller()).collect(Collectors.toList());
        if  (!controllersToRestart.isEmpty()) {
            return nextControllerToRestart(reconciliation, rollClient, controllersToRestart);
        }

        return nextBatchBrokers(reconciliation, rollClient, nodesToRestart, maxRestartBatchSize);
    }

    /**
     * @param reconciliation The roll client
     * @param controllersToStart controllers to restarts
     * @return The first one from the given list of nodes that can be restarted without impacting the quorum health.
     * If there is no node that doesn't have an impact on the quorum health, an empty set is returned.
     */
    private Set<Context> nextControllerToRestart(Reconciliation reconciliation, RollClient rollClient,
                                                 List<Context> controllersToStart) {

        int activeControllerId = rollClient.activeController();
        LOGGER.debugCr(reconciliation, "The active controller is {}", activeControllerId);

        if (activeControllerId < 0) {
            // if we can't determine the active controller, we cannot safely restart a controller node
            // To retry, we return an empty set
            return Collections.emptySet();
        }

        var orderedNodes = controllersToStart.stream().sorted(Comparator.comparing((Context c) -> c.state().equals(State.READY)) // Sort by the state (ready goes to the back)
                .thenComparing(c -> c.currentRoles().broker()));  // Sort by the roles (combined goes to the back)))
        LOGGER.debugCr(reconciliation, "Checking controllers in the following order to restart: {}", controllersToStart);

        Set<Context> nextNodeToRestart = new HashSet<>();
        orderedNodes.anyMatch(c -> {
            if (c.nodeId() == activeControllerId && controllersToStart.size() != 1) {
                LOGGER.debugCr(reconciliation, "Controller node {} is the active controller, there are other controller nodes to restart", c.nodeId());
                return false;
            }

            if (isQuorumHealthyWithoutNode(reconciliation, c.nodeId(), activeControllerId, rollClient)) {
                // if this node is combined, then we have to check the availability as well
                if (c.currentRoles().broker()) {
                    try {
                        if (new Availability(reconciliation, rollClient).anyPartitionWouldBeUnderReplicated(c.nodeId())) {
                            LOGGER.debugCr(reconciliation, "Combined node {} cannot be safely restarted without impacting the availability", c.nodeId());
                            return false;
                        }
                    } catch (Exception e) {
                        LOGGER.errorCr(reconciliation, "Failed checking availability of topic partitions", e);
                        return false;
                    }
                }

                LOGGER.debugCr(reconciliation, "Controller node {} can be safely restarted", c.nodeId());
                nextNodeToRestart.add(c);
                return true;
            } else {
                LOGGER.debugCr(reconciliation, "Controller node {} cannot be safely restarted without impacting quorum health", c.nodeId());
            }
            return false;
        });

        if (nextNodeToRestart.isEmpty()) LOGGER.warnCr(reconciliation, "None of the following controller nodes can be safely restarted: {}", controllersToStart);
        return nextNodeToRestart;
    }

    /**
     * Returns true if the majority of the controllers' lastCaughtUpTimestamps are within
     * the controller.quorum.fetch.timeout.ms based on the given quorum info.
     * The given controllerNeedRestarting is the one being considered to restart, therefore excluded from the check.
     *
     * The total number of controller is passed in to this method rather than using the size of the quorum followers
     * returned from the Admin. This is because when scaling down controllers, the returned quorum info from them
     * could contain inconsistent number of followers.
     */
    private boolean isQuorumHealthyWithoutNode(Reconciliation reconciliation,
                                               int controllerNeedRestarting,
                                               int activeControllerId,
                                               RollClient rollClient) {
        LOGGER.debugCr(reconciliation, "Determining the impact of restarting controller {} on quorum health", controllerNeedRestarting);
        Map<Integer, Long> quorumFollowerStates = rollClient.quorumLastCaughtUpTimestamps(contexts.stream().filter(c -> c.nodeId() == activeControllerId).map(Context::nodeRef).collect(Collectors.toSet()));
        int controllerCount = quorumFollowerStates.size();
        if (controllerCount == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on controller quorum with a single node. The cluster may be " +
                    "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
            return true;
        }

        // Get the NodeRef for the active controller to describe its configs.
        // If config contains controller.quorum.fetch.timeout.ms, use it for the quorum check,
        // otherwise, use the hard-coded default value.
        var activeController = contexts.stream().filter(c -> c.nodeId() == activeControllerId).map(Context::nodeRef).toList();
        var config = rollClient.describeControllerConfigs(activeController);
        var controllerQuorumFetchTimeout = (config != null) && (config.get(0) != null) ?
                Long.parseLong(config.get(0).get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME).value()) : CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

        long leaderLastCaughtUpTimestamp = quorumFollowerStates.get(activeControllerId);

        long numOfCaughtUpControllers = quorumFollowerStates.entrySet().stream().filter(entry -> {
            int nodeId = entry.getKey();
            long lastCaughtUpTimestamp = entry.getValue();
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.errorCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", nodeId);
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
     * Returns a batch of broker nodes that have no topic partitions in common and have no impact on cluster availability if restarted.
     */
    private Set<Context> nextBatchBrokers(Reconciliation reconciliation,
                                                   RollClient rollClient,
                                                   List<Context> nodesNeedingRestart,
                                                   int maxRestartBatchSize) {
        if (nodesNeedingRestart.size() < 1) {
            return Collections.emptySet();
        }

        Availability availability;
        try {
            availability = new Availability(reconciliation, rollClient);
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Failed checking availability of topic partitions", e);
            return Set.of();
        }

        // If maxRestartBatchSize is set to 1, no point executing batching algorithm so
        // return the next available node that is ordered by the readiness state
        if (maxRestartBatchSize == 1) {
            List<Context> eligibleNodes = nodesNeedingRestart.stream()
                    .filter(context -> !availability.anyPartitionWouldBeUnderReplicated(context.nodeId()))
                    .sorted(Comparator.comparing((Context c) -> c.state().equals(State.READY)))
                    .toList();
            return eligibleNodes.size() > 0 ?  Set.of(eligibleNodes.get(0)) : Set.of();
        }

        LOGGER.debugCr(reconciliation, "Parallel batching of broker nodes is enabled. Max batch size is {}");
        List<KafkaNode> nodes = nodesNeedingRestart.stream()
                .map(c -> new KafkaNode(c.nodeId(), availability.getReplicasForNode(c.nodeId())))
                .collect(Collectors.toList());

        // Split the set of all brokers into subsets of brokers that can be rolled in parallel
        var cells = Batching.cells(reconciliation, nodes);
        int cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Cell {}: {}", ++cellNum, cell);
        }

        cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Restart-eligible cell {}: {}", ++cellNum, cell);
        }

        var batches = Batching.batchCells(reconciliation, cells, availability, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Batches {}", Batching.nodeIdsToString2(batches));

        var bestBatch = Batching.pickBestBatchForRestart(batches);
        LOGGER.debugCr(reconciliation, "Best batch {}", Batching.nodeIdsToString(bestBatch));

        return nodesNeedingRestart.stream().filter(c -> bestBatch.contains(c.nodeId())).collect(Collectors.toSet());
    }

}
