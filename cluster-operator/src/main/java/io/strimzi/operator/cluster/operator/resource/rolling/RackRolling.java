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
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaAvailability;
import io.strimzi.operator.cluster.operator.resource.KafkaConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaQuorumCheck;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.QuorumInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.unwrap;

/**
 * RackRolling
 */
@SuppressWarnings({"ParameterNumber" })
public class RackRolling {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;
    private final List<Context> contexts;

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * @param reconciliation           Reconciliation marker
     * @param vertx                    Vert.x instance
     * @param podOperator              Pod operator for managing pods
     * @param pollingIntervalMs        The polling interval in milliseconds for checking node state
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
     * @param maxRestartBatchSize      The maximum number of nodes that might be restarted at once
     * @param eventPublisher           Kubernetes Events publisher for publishing events about node restarts
     * @return RackRolling instance
     */
    public static RackRolling initialise(Reconciliation reconciliation,
                                         Vertx vertx,
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
                                         int maxRestartBatchSize,
                                         KubernetesRestartEventPublisher eventPublisher) {
        PlatformClient platformClient = new PlatformClientImpl(podOperator, reconciliation.namespace(), reconciliation, postOperationTimeoutMs, eventPublisher);
        Time time = Time.SYSTEM_TIME;
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        RollClient rollClient = new RollClientImpl(reconciliation, coTlsPemIdentity, adminClientProvider);
        AgentClient agentClient = new AgentClientImpl(kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity));

        return new RackRolling(time,
                reconciliation,
                vertx,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                contexts,
                platformClient,
                rollClient,
                agentClient,
                kafkaConfigProvider, kafkaVersion,
                allowReconfiguration
        );
    }

    // visible for testing
    static RackRolling initialise(Time time,
                                            Vertx vertx,
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
                                            Supplier<BackOff> backOffSupplier) {
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        return new RackRolling(time,
                reconciliation,
                vertx,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
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
    private final Vertx vertx;
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
    private long controllerQuorumFetchTimeoutMs;
    private KafkaAvailability kafkaAvailability;
    private KafkaQuorumCheck kafkaQuorumCheck;
    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
            runnable -> new Thread(runnable, "kafka-roller"));

    /**
     * Constructor for RackRolling instance
     *
     * @param time                   initial time to set for context
     * @param reconciliation         Reconciliation marker
     * @param vertx                  Vert.x instance
     * @param pollingIntervalMs      The polling interval in milliseconds for checking node state
     * @param postOperationTimeoutMs The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize    The maximum number of nodes that might be restarted at once
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
                       Vertx vertx,
                       long pollingIntervalMs,
                       long postOperationTimeoutMs,
                       int maxRestartBatchSize,
                       List<Context> contexts,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       AgentClient agentClient,
                       Function<Integer, String> kafkaConfigProvider,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration) {
        this.time = time;
        this.vertx = vertx;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.agentClient = agentClient;
        this.reconciliation = reconciliation;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.postOperationTimeoutMs = postOperationTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.contexts = contexts;
        this.allowReconfiguration = allowReconfiguration;

        // Initialize controllerQuorumFetchTimeoutMs - will be set properly in maybeInitAdminClients
        this.controllerQuorumFetchTimeoutMs = CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

        // Initialize KafkaAvailability and KafkaQuorumCheck
        // Note: These will be initialized lazily when admin clients are available
        this.kafkaAvailability = null;
        this.kafkaQuorumCheck = null;
    }

    /**
     * Runs the roller via single thread Executor
     *
     * @return a future based on the rolling outcome.
     */
    public Future<Void> rollingRestart() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        singleExecutor.submit(() -> {
            try {
                schedule(0, result)
                .whenComplete((i, error) -> {
                    singleExecutor.shutdown();

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

                    if (error != null) {
                        result.completeExceptionally(unwrap(error));
                    } else {
                        result.complete(null);
                    }
                });
            } catch (Exception e)   {
                // If anything happens, we have to raise the error otherwise the reconciliation would get stuck
                // Its logged at upper level, so we just log it at debug here
                LOGGER.debugCr(reconciliation, "Something went wrong when trying to do a rolling restart", e);
                singleExecutor.shutdown();
                result.completeExceptionally(unwrap(e));
            }
        });

        return VertxUtil.completableFutureToVertxFuture(result);
    }

    private CompletableFuture<Void> schedule(long delay,  CompletableFuture<Void> scheduleResult) {
        singleExecutor.schedule(() -> {
            reconcile().whenComplete((i, error) -> {
                long delayMs = 0;
                if (error == null) {
                    scheduleResult.complete(null);
                } else {
                    Throwable cause = unwrap(error);
                    if (cause instanceof RetriableException) {
                        List<Context> nodesToRetry = new ArrayList<>();

                        for (Context context : contexts) {
                            if (context.shouldRetry()) {
                                if (context.backOff().done()) {
                                    LOGGER.infoCr(reconciliation, "Could not verify pod {} is up-to-date, giving up after {} attempts. Total delay between attempts {}ms",
                                            context.nodeRef(), context.backOff().maxAttempts(), context.backOff().totalDelayMs(), cause);
                                    scheduleResult.completeExceptionally(new MaxAttemptsExceededException());
                                    return;
                                } else {
                                    long delay1 = context.backOff().delayMs();
                                    if (delay1 > delayMs) {
                                        delayMs = delay1;
                                    }
                                    LOGGER.infoCr(reconciliation, "Will temporarily skip verifying pod {} is up-to-date due to {}, retrying after at least {}ms",
                                            context.nodeRef(), cause, delay1);
                                    nodesToRetry.add(context);
                                }
                            }
                        }

                        if (!nodesToRetry.isEmpty()) {
                            schedule(delayMs, scheduleResult);
                        } else {
                            scheduleResult.completeExceptionally(new RuntimeException("Caught RetriableException but no pods to retry", cause));
                        }
                    } else {
                        LOGGER.infoCr(reconciliation, "Could not reconcile pods, giving up without retrying because we encountered a fatal error", cause);
                        scheduleResult.completeExceptionally(cause);
                        singleExecutor.shutdownNow();
                    }
                }
            });
        }, delay, TimeUnit.MILLISECONDS);

        return scheduleResult;
    }

    public CompletableFuture<Void> reconcile() {
        return initialiseContexts()
                .thenCompose(i -> waitForLogRecovery())
                .thenCompose(i -> maybeForceRestartNodes())
                .thenCompose(i -> maybeInitAdminClients())
                .thenCompose(i -> reconfigureNodes())
                .thenCompose(i -> restartControllers())
                .thenCompose(i -> restartBrokers())
                .thenCompose(i -> finalCheck());

        //TODO:
        // InterruptedException is thrown from await method() and is handled in schedule() method
    }

    private CompletableFuture<Void> maybeInitAdminClients() {
        try {
            rollClient.initialiseControllerAdmin(contexts.stream().filter(c -> c.currentRoles().controller()).map(Context::nodeRef).collect(Collectors.toSet()));
            rollClient.initialiseBrokerAdmin(contexts.stream().filter(c -> c.currentRoles().broker()).map(Context::nodeRef).collect(Collectors.toSet()));

            // Initialize KafkaAvailability with broker admin client
            if (rollClient.getBrokerAdminClient() != null) {
                kafkaAvailability = new KafkaAvailability(reconciliation,rollClient.getBrokerAdminClient());
            }

            // Get controller quorum fetch timeout and initialize KafkaQuorumCheck
            if (rollClient.getControllerAdminClient() != null) {
                var quorumInfo = rollClient.describeMetadataQuorum();
                var config = rollClient.describeControllerConfigs(quorumInfo.leaderId());
                controllerQuorumFetchTimeoutMs = (config != null) ?
                        Long.parseLong(config.get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME).value()) : CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

                kafkaQuorumCheck = new KafkaQuorumCheck(reconciliation, rollClient.getControllerAdminClient(), vertx, controllerQuorumFetchTimeoutMs);
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(new RetriableException("Unable to initialise admin clients", e));
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> finalCheck() {
        List<Context> notReadyNodes = new ArrayList<>();
        long remainingTimeoutMs = postOperationTimeoutMs;
        // By testing even pods which have no reasons to restart for readiness we prevent successive reconciliations
        // from taking out a pod each time (due, e.g. to a configuration error).
        // We rely on Kube to try restarting such pods.
        for (Context context : contexts) {
            if (context.state() != State.READY) {
                LOGGER.debugCr(reconciliation, "Pod {} does not need to be restarted", context.nodeRef());
                try {
                    LOGGER.debugCr(reconciliation, "Waiting for non-restarted pod {} to become ready", context.nodeRef());
                    remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
                } catch (TimeoutException e) {
                    notReadyNodes.add(context);
                }
                LOGGER.debugCr(reconciliation, "Pod {} is now ready", context.nodeRef());
            }
        }

        if (!notReadyNodes.isEmpty()) {
            return CompletableFuture.failedFuture(new UnretriableException("Error while waiting for non restarted pod/s: " + notReadyNodes));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> initialiseContexts() {
        // Observe current state and update the contexts
        for (var context : contexts) {
            context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            context.setRetryFlag(false);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, AgentClient agentClient, NodeRef nodeRef) {
        State state;
        var nodeState = platformClient.nodeState(nodeRef);
        if (nodeState == null) {
            LOGGER.debugCr(reconciliation, "Pod {} doesn't exist. There seems to be some problem with the creation of pod by StrimziPodSets controller", nodeRef);
            return State.UNKNOWN;
        }

        LOGGER.debugCr(reconciliation, "Pod {}: State is {}", nodeRef, nodeState);
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
                    LOGGER.debugCr(reconciliation, "Pod {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() >= BrokerState.RUNNING.value() && bs.value() != BrokerState.UNKNOWN.value()) {
                        state = State.READY;
                    } else if (bs.value() == BrokerState.RECOVERY.value()) {
                        LOGGER.warnCr(reconciliation, "Pod {} is not ready because the Kafka is performing log recovery. There are {} logs and {} segments left to recover", nodeRef, bs.remainingLogsToRecover(), bs.remainingSegmentsToRecover());
                        state = State.RECOVERING;
                    } else {
                        state = State.NOT_READY;
                    }
                } catch (Exception e) {
                    //TODO: since observe method is called very frequently, maybe we should not print the full error cause in each message, maybe just in debug?
                    LOGGER.warnCr(reconciliation, "Could not get broker state for pod {}. This might be temporary if a pod was just restarted", nodeRef, e);
                    state = State.NOT_READY;
                }
        }
        LOGGER.debugCr(reconciliation, "Pod {}: observation outcome is {}", nodeRef, state);
        return state;
    }


    private long awaitReadyState(Context context, long timeoutMs) throws TimeoutException {
        LOGGER.infoCr(reconciliation, "Pod {}: Waiting for pod to enter state {}", context.nodeRef(), State.READY);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + State.READY + " within " + timeoutMs + " ms: " + context
        ).poll(pollingIntervalMs, () -> {
            var state = context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            return state == State.READY;
        });
    }


    private CompletableFuture<Void> waitForLogRecovery() {
        long remainingTimeoutMs = postOperationTimeoutMs;

        for (Context context : contexts) {
                try {
                    remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
                } catch (TimeoutException e) {
                    var brokerState = agentClient.getBrokerState(context.nodeRef());
                    if  (brokerState == BrokerState.RECOVERY) {
                        context.setRetryFlag(true);
                        return CompletableFuture.failedFuture(new RetriableException("Pod " + context.nodeRef() + " is not ready because the Kafka is performing log recovery. There are " + brokerState.remainingLogsToRecover() + " logs and " + brokerState.remainingSegmentsToRecover() + " segments left to recover.", e));
                    } else {
                        LOGGER.warnCr(reconciliation, "Pod {} is not ready. We will check if KafkaRoller can do anything about it.", context.nodeRef().podName());
                    }
                }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> maybeForceRestartNodes() {
        List<Context> controllersToRestart = new ArrayList<>();
        List<Context> brokersToRestart = new ArrayList<>();

        for (var c : contexts) {
            if (c.state().equals(State.NOT_RUNNING)) {
                if (c.reason().contains(RestartReason.POD_HAS_OLD_REVISION)) {
                    LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it seems to be stuck and restart might help", c.nodeRef());
                    if (c.currentRoles().controller()) {
                        controllersToRestart.add(c);
                    } else {
                        brokersToRestart.add(c);
                    }
                } else {
                    return CompletableFuture.failedFuture(new UnretriableException("Pod " + c.nodeRef().podName() + " is unschedulable or is not starting"));
                }
            } else if (!rollClient.canConnectToNode(c.nodeRef(), c.currentRoles().broker())) {
                LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it does not seem to responding to connection attempts", c.nodeRef());
                if (c.currentRoles().controller()) {
                    controllersToRestart.add(c);
                } else {
                    brokersToRestart.add(c);
                }
            }
        }

        if (controllersToRestart.isEmpty() && brokersToRestart.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> restartResult;
            if (!controllersToRestart.isEmpty()) {
                if (controllersToRestart.size() > 1) {
                    LOGGER.warnCr(reconciliation, "There are multiple controller pods that are not running, which runs a risk of losing the quorum. Restarting them in parallel: {}", controllersToRestart);
                }
                restartResult = restartInParallelAndAwaitReadiness(controllersToRestart);
            } else {
                Context broker = brokersToRestart.getFirst();
                restartResult = restartNodeAndAwaitReadiness(broker);
                brokersToRestart.remove(broker);
            }

            return restartResult.thenCompose((i) -> {
                if (brokersToRestart.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return CompletableFuture.failedFuture(new RetriableException("There are more pods that may still need to be force restarted: " + brokersToRestart));
                }
            });
        }
    }

    private CompletableFuture<Void> restartNodeAndAwaitReadiness(Context context) {
        try {
            restartNode(context);
        } catch (Exception e) {
            context.setRetryFlag(true);
            return CompletableFuture.failedFuture(new RetriableException("Error while trying to restart pod " + context.nodeRef().podName() + " to become ready: " + e));
        }

        long remainingTimeoutMs = postOperationTimeoutMs;
        try {
            remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new UnretriableException("Error while waiting for restarted pod " + context.nodeRef().podName() + " to become ready: " + e));
        }

        awaitPreferred(context, remainingTimeoutMs);

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> restartInParallelAndAwaitReadiness(List<Context> batch) {
        for (Context context : batch) {
            try {
                restartNode(context);
            } catch (Exception e) {
                context.setRetryFlag(true);
                return CompletableFuture.failedFuture(new RetriableException("Error while trying to restart pod " + context.nodeRef().podName() + " to become ready: " + e));
            }
        }

        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context context : batch) {
            try {
                remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
                if (context.currentRoles().broker()) {
                    awaitPreferred(context, remainingTimeoutMs);
                }
            } catch (Exception e) {
                return CompletableFuture.failedFuture(new UnretriableException("Error while waiting for restarted pod " + context.nodeRef().podName() + " to become ready: " + e));
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private void restartNode(Context context) {
        LOGGER.infoCr(reconciliation, "Pod {}: Restarting", context.nodeRef());
        platformClient.restartNode(context.nodeRef(), context.reason());
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.infoCr(reconciliation, "Pod {}: Restarted", context.nodeRef());
    }

    private void awaitPreferred(Context context, long timeoutMs) {
        // TODO: apply configured delay (via env variable) before triggering leader election.
        //  This should be probably passed to tryElectAllPreferredLeaders so that delay is only applied
        //  if there are topic partitions to elect, otherwise no point of delaying the process
        time.sleep(10000L, 0);
        LOGGER.debugCr(reconciliation, "Pod {}: Waiting for Kafka broker to be leader of all its preferred replicas", context.nodeRef());
        try {
            Alarm.timer(time,
                            timeoutMs,
                            () -> "Failed to elect the preferred leader " + context.nodeRef() + " for topic partitions within " + timeoutMs)
                    .poll(pollingIntervalMs, () -> rollClient.tryElectAllPreferredLeaders(context.nodeRef()) == 0);
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for pod " + context.nodeRef() + " to be leader for all its preferred replicas");
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Failed to elect preferred replica", e);
        }
    }

    private CompletableFuture<Void> reconfigureNodes() {
        if (allowReconfiguration) {
            for (Context context : contexts) {
                if (!context.reason().getReasons().isEmpty()) {
                    Config configs;

                    try {
                        if (context.currentRoles().broker()) {
                            //TODO: create one admin call , if that fails, then make individual calls for the config
                            configs = rollClient.describeBrokerConfigs(context.nodeId());
                        } else {
                            configs = rollClient.describeControllerConfigs(context.nodeId());
                        }
                    } catch (Exception e) {
                        LOGGER.warnCr(reconciliation, "Error getting getting Kafka configs for {}.", context.nodeRef());
                        context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                        continue;
                    }

                    var diff = new KafkaConfigurationDiff(reconciliation,
                            configs,
                            kafkaConfigProvider.apply(context.nodeId()),
                            kafkaVersion,
                            context.nodeRef(),
                            context.currentRoles().controller(),
                            context.currentRoles().broker());

                    if (!diff.isEmpty()) {
                        if (diff.canBeUpdatedDynamically()) {
                            try {
                                //TODO: can we do joint call or has to be done singular?
                                reconfigureNode(reconciliation, time, rollClient, context, diff);
                            } catch (Exception e) {
                                LOGGER.warnCr(reconciliation, "Failed to reconfigure {} due to {} therefore will restart", context.nodeRef(), e);
                                context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                            }
                        } else {
                            context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                        }
                    }
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private static void reconfigureNode(Reconciliation reconciliation,
                                        Time time,
                                        RollClient rollClient,
                                        Context context,
                                        KafkaConfigurationDiff configDiff) {
        LOGGER.debugCr(reconciliation, "Pod {}: Reconfiguring", context.nodeRef());
        rollClient.reconfigureNode(context.nodeRef(), configDiff, context.currentRoles().broker());
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Pod {}: Reconfigured", context.nodeRef());
    }

    private CompletableFuture<Void> restartControllers() {
        var orderedContexts = new ArrayList<>(contexts.stream()
                .filter(c -> !c.reason().getReasons().isEmpty() && c.currentRoles().controller())
                .sorted(Comparator.comparing((Context c) -> c.currentRoles().broker()) // Sort by the roles (combined goes to the back)
                        .thenComparing(c -> c.state().equals(State.READY))) // Sort by the state (ready goes to the back)
                .toList());

        if (orderedContexts.isEmpty()) {
            LOGGER.debugCr(reconciliation, "There are no controller pods that need to be restarted");
            return CompletableFuture.completedFuture(null);
        }

        var quorumInfo = rollClient.describeMetadataQuorum();
        var config = rollClient.describeControllerConfigs(quorumInfo.leaderId());
        var controllerQuorumFetchTimeout = (config != null) ?
                Long.parseLong(config.get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME).value()) : CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;

        Availability availability = new Availability(reconciliation, rollClient);
        CompletableFuture<Void> restartResult = null;
        for (Context context : orderedContexts) {
            if (isQuorumHealthyWithoutNode(context.nodeId(), quorumInfo, controllerQuorumFetchTimeout)) {
                if (context.currentRoles().broker()) {
                    if (availability.anyPartitionWouldBeUnderReplicated(context.nodeId())) {
                        LOGGER.debugCr(reconciliation, "Pod {} cannot be restarted safely right now", context.nodeRef());
                        context.setRetryFlag(true);
                    } else {
                        restartResult = restartNodeAndAwaitReadiness(context);
                        orderedContexts.remove(context);
                        break;
                    }
                } else {
                    restartResult = restartNodeAndAwaitReadiness(context);
                    orderedContexts.remove(context);
                    break;
                }
            } else {
                LOGGER.debugCr(reconciliation, "Pod {} cannot be restarted safely right now", context.nodeRef());
                context.setRetryFlag(true);
            }
        }

        // If no context was restarted, all contexts need retry
        if (restartResult != null) {
            return restartResult.thenCompose((i) -> {
                if (orderedContexts.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return CompletableFuture.failedFuture(new RetriableException("There are more controller pods that may need restarting: " + orderedContexts));
                }
            });
        } else {
            return CompletableFuture.failedFuture(new RetriableException("There are more controller pods that may need restarting: " + orderedContexts));
        }
    }

    private CompletableFuture<Void> restartBrokers() {
        var orderedContexts = new ArrayList<>(contexts.stream()
                .filter(c -> !c.reason().getReasons().isEmpty() && c.currentRoles().broker())
                .sorted(Comparator.comparing((Context c) -> c.state().equals(State.READY)))// Sort by the state (ready goes to the back)
                .toList());

        if (orderedContexts.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Availability availability; // TODO: I think we should separate availability check from the batching so that we do topic describe and pass it avail check and batching
        try {
            availability = new Availability(reconciliation, rollClient);
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Failed checking availability of topic partitions", e);
            return CompletableFuture.failedFuture(new RetriableException("Failed checking availability of topic partitions", e));
        }

        List<Context> nodesToBatch = new ArrayList<>();
        for (Context context : orderedContexts) {
            if (availability.anyPartitionWouldBeUnderReplicated(context.nodeId())) {
                context.setRetryFlag(true);
            } else {
                nodesToBatch.add(context);
            }
        }

        if (nodesToBatch.isEmpty()) {
            return CompletableFuture.failedFuture(new RetriableException("Pods cannot be safely restarted: " + orderedContexts));
        }

        CompletableFuture<Void> restartResult;
        if (maxRestartBatchSize > 1) {
            var nextBatchBrokers = nextBatchBrokers(reconciliation, availability, nodesToBatch, maxRestartBatchSize);
            restartResult = restartInParallelAndAwaitReadiness(nextBatchBrokers);
            orderedContexts.removeAll(nextBatchBrokers);
        } else {
            Context broker = nodesToBatch.getFirst();
            restartResult = restartNodeAndAwaitReadiness(broker);
            orderedContexts.remove(broker);
        }

        return restartResult.thenCompose((i) -> {
            if (orderedContexts.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            } else {
                return CompletableFuture.failedFuture(new RetriableException("There are more broker pods that may need restarting: " + orderedContexts));
            }
        });
    }

    /**
     * Returns a batch of broker pods that have no topic partitions in common and have no impact on cluster availability if restarted.
     */
    private List<Context> nextBatchBrokers(Reconciliation reconciliation,
                                                   Availability availability,
                                                   List<Context> nodesNeedingRestart,
                                                   int maxRestartBatchSize) {
        LOGGER.debugCr(reconciliation, "Parallel batching of broker pods is enabled. Max batch size is {}", maxRestartBatchSize);
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

        // Check if any fail avail, mark retry and add to unavail
        var batches = Batching.batchCells(reconciliation, cells, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Batches {}", Batching.nodeIdsToString2(batches));

        var bestBatch = Batching.pickBestBatchForRestart(batches);
        LOGGER.debugCr(reconciliation, "Best batch {}", Batching.nodeIdsToString(bestBatch));

        return nodesNeedingRestart.stream().filter(c -> bestBatch.contains(c.nodeId())).toList();
    }

    private boolean isQuorumHealthyWithoutNode(int controllerNeedRestarting,
                                               QuorumInfo info,
                                               long controllerQuorumFetchTimeoutMs) {
        int leaderId = info.leaderId();
        if (leaderId < 0) {
            LOGGER.warnCr(reconciliation, "No controller quorum leader is found because the leader id is set to {}", leaderId);
            return false;
        }

        Map<Integer, Long> controllerStates = info.voters().stream().collect(Collectors.toMap(
                QuorumInfo.ReplicaState::replicaId,
                state -> state.lastCaughtUpTimestamp().isPresent() ? state.lastCaughtUpTimestamp().getAsLong() : -1));
        int totalNumOfControllers = controllerStates.size();

        if (totalNumOfControllers == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with a single node. This may result in data loss " +
                    "or may cause disruption to the cluster during the rolling update. It is recommended that a minimum of three controllers are used.");
            return true;
        }

        long leaderLastCaughtUpTimestamp = controllerStates.get(leaderId);
        LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for the controller quorum leader (node id {}) is {}", leaderId, leaderLastCaughtUpTimestamp);

        long numOfCaughtUpControllers = controllerStates.entrySet().stream().filter(entry -> {
            int controllerNodeId = entry.getKey();
            long lastCaughtUpTimestamp = entry.getValue();
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", controllerNodeId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", controllerNodeId, lastCaughtUpTimestamp);
                if (controllerNodeId == leaderId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < controllerQuorumFetchTimeoutMs) {

                    // skip the controller that we are considering to roll
                    if (controllerNodeId != controllerNeedRestarting) {
                        return true;
                    }
                    LOGGER.debugCr(reconciliation, "Controller {} has caught up with the controller quorum leader", controllerNodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "Controller {} has fallen behind the controller quorum leader", controllerNodeId);
                }
            }
            return false;
        }).count();

        LOGGER.debugCr(reconciliation, "Out of {} controllers, there are {} that have caught up with the controller quorum leader, not including controller {}", totalNumOfControllers, numOfCaughtUpControllers, controllerNeedRestarting);

        if (totalNumOfControllers == 2) {

            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. This may result in data loss  " +
                        "or cause disruption to the cluster during the rolling update. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            return numOfCaughtUpControllers >= (totalNumOfControllers + 2) / 2;
        }
    }

}
