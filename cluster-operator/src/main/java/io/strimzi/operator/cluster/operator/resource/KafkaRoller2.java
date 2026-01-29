/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.SslAuthenticationException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.unwrap;

/**
 * KafkaRoller 2
 */
@SuppressWarnings({"ParameterNumber" })
public class KafkaRoller2 {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaRoller2.class);
    private final List<RestartContext> restartContexts;

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * @param reconciliation           Reconciliation marker
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
    public static KafkaRoller2 initialise(Reconciliation reconciliation,
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
        final var contexts = nodes.stream().map(node -> RestartContext.start(node, platformClient.kafkaNodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        KafkaRollerClient kafkaRollerClient = new KafkaRollerClientImpl(reconciliation, coTlsPemIdentity, adminClientProvider);
        KafkaAgentClient kafkaAgentClient = kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity);

        return new KafkaRoller2(time,
                reconciliation,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                contexts,
                platformClient,
                kafkaRollerClient,
                kafkaAgentClient,
                kafkaConfigProvider,
                kafkaVersion,
                allowReconfiguration
        );
    }

    // visible for testing
    static KafkaRoller2 initialise(Time time,
                                   PlatformClient platformClient,
                                   KafkaRollerClient kafkaRollerClient,
                                   KafkaAgentClient kafkaAgentClient,
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
        final var contexts = nodes.stream().map(node -> RestartContext.start(node, platformClient.kafkaNodeRoles(node), predicate, backOffSupplier, podOperator, reconciliation.namespace(), time)).collect(Collectors.toList());

        return new KafkaRoller2(time,
                reconciliation,
                pollingIntervalMs,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                contexts,
                platformClient,
                kafkaRollerClient,
                kafkaAgentClient,
                kafkaConfigProvider,
                kafkaVersion,
                allowReconfiguration
        );
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final KafkaRollerClient kafkaRollerClient;
    private final KafkaAgentClient kafkaAgentClient;
    private final Reconciliation reconciliation;
    private final KafkaVersion kafkaVersion;
    private final boolean allowReconfiguration;
    private final Function<Integer, String> kafkaConfigProvider;
    private final long postOperationTimeoutMs;
    private final int maxRestartBatchSize;
    private final long pollingIntervalMs;
    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
            runnable -> new Thread(runnable, "kafka-roller"));

    /**
     * Constructor for RackRolling instance
     *
     * @param time                   initial time to set for context
     * @param reconciliation         Reconciliation marker
     * @param pollingIntervalMs      The polling interval in milliseconds for checking node state
     * @param postOperationTimeoutMs The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize    The maximum number of nodes that might be restarted at once
     * @param restartContexts        List of context for each node
     * @param platformClient         client for platform calls
     * @param kafkaRollerClient      client for kafka cluster admin calls
     * @param kafkaAgentClient       client for kafka agent calls
     * @param kafkaConfigProvider    Kafka configuration provider
     * @param kafkaVersion           Kafka version
     * @param allowReconfiguration   Flag indicting whether reconfiguration is allowed or not
     */
    public KafkaRoller2(Time time,
                        Reconciliation reconciliation,
                        long pollingIntervalMs,
                        long postOperationTimeoutMs,
                        int maxRestartBatchSize,
                        List<RestartContext> restartContexts,
                        PlatformClient platformClient,
                        KafkaRollerClient kafkaRollerClient,
                        KafkaAgentClient kafkaAgentClient,
                        Function<Integer, String> kafkaConfigProvider,
                        KafkaVersion kafkaVersion,
                        boolean allowReconfiguration) {
        this.time = time;
        this.platformClient = platformClient;
        this.kafkaRollerClient = kafkaRollerClient;
        this.kafkaAgentClient = kafkaAgentClient;
        this.reconciliation = reconciliation;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.postOperationTimeoutMs = postOperationTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.restartContexts = restartContexts;
        this.allowReconfiguration = allowReconfiguration;
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
                                kafkaRollerClient.closeControllerAdminClient();
                            } catch (RuntimeException e) {
                                LOGGER.debugCr(reconciliation, "Exception closing controller admin client", e);
                            }

                            try {
                                kafkaRollerClient.closeBrokerAdminClient();
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
                    if (cause instanceof FatalException) {

                        LOGGER.infoCr(reconciliation, "Could not reconcile pods, giving up without retrying because we encountered a fatal error", cause);
                        scheduleResult.completeExceptionally(cause);
                        singleExecutor.shutdownNow();

                    } else {
                        for (RestartContext restartContext : restartContexts) {
                            if (restartContext.shouldRetry()) {
                                if (restartContext.backOff().done()) {
                                    LOGGER.infoCr(reconciliation, "Could not verify pod {} is up-to-date, giving up after {} attempts. Total delay between attempts {}ms",
                                            restartContext.nodeRef(), restartContext.backOff().maxAttempts(), restartContext.backOff().totalDelayMs(), cause);
                                    scheduleResult.completeExceptionally(new io.strimzi.operator.common.TimeoutException(cause.getMessage()));
                                    return;
                                } else {
                                    long delay1 = restartContext.backOff().delayMs();
                                    if (delay1 > delayMs) {
                                        delayMs = delay1;
                                    }
                                    LOGGER.infoCr(reconciliation, "Will temporarily skip verifying pod {} is up-to-date due to {}, retrying after at least {}ms",
                                            restartContext.nodeRef(), cause, delay1);
                                }
                            }
                        }

                        schedule(delayMs, scheduleResult);
                    }
                }
            });
        }, delay, TimeUnit.MILLISECONDS);

        return scheduleResult;
    }

    /**
     * Reconciles pods that may need to be restarted, reconfigured or fixed if not healthy.
     * @return CompletableFuture immediately if any of the reconcile steps fail or once all of them complete successfully
     */
    public CompletableFuture<Void> reconcile() {
        return initialiseContexts()
                .thenCompose(i -> waitForLogRecovery())
                .thenCompose(i -> maybeForceRestartNodes())
                .thenCompose(i -> maybeInitAdminClients())
                .thenCompose(i -> reconfigureNodes())
                .thenCompose(i -> restartControllers())
                .thenCompose(i -> restartBrokers())
                .thenCompose(i -> finalCheck());
    }

    private CompletableFuture<Void> maybeInitAdminClients() {
        try {
            kafkaRollerClient.initialiseControllerAdmin(restartContexts.stream().filter(c -> c.currentRoles().controller()).map(RestartContext::nodeRef).collect(Collectors.toSet()));
            kafkaRollerClient.initialiseBrokerAdmin(restartContexts.stream().filter(c -> c.currentRoles().broker()).map(RestartContext::nodeRef).collect(Collectors.toSet()));
        } catch (RuntimeException e) {
            restartContexts.forEach(restartContext -> restartContext.setRetryFlag(true));
            return CompletableFuture.failedFuture(new RetriableException("Unable to initialise admin clients", e));
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> finalCheck() {
        List<RestartContext> notReadyNodes = new ArrayList<>();
        long remainingTimeoutMs = postOperationTimeoutMs;
        // By testing even pods which have no reasons to restart for readiness we prevent successive reconciliations
        // from taking out a pod each time (due, e.g. to a configuration error).
        // We rely on Kube to try restarting such pods.
        for (RestartContext restartContext : restartContexts) {
            if (restartContext.state() != State.READY) {
                LOGGER.debugCr(reconciliation, "Pod {} does not need to be restarted", restartContext.nodeRef());
                try {
                    LOGGER.debugCr(reconciliation, "Waiting for non-restarted pod {} to become ready", restartContext.nodeRef());
                    remainingTimeoutMs = awaitReadyState(restartContext, remainingTimeoutMs);
                } catch (TimeoutException e) {
                    notReadyNodes.add(restartContext);
                }
                LOGGER.debugCr(reconciliation, "Pod {} is now ready", restartContext.nodeRef());
            }
        }

        if (!notReadyNodes.isEmpty()) {
            return CompletableFuture.failedFuture(new FatalException("Error while waiting for non restarted pod/s: " + notReadyNodes));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> initialiseContexts() {
        // Observe current state and update the contexts
        for (var context : restartContexts) {
            context.transitionTo(observe(reconciliation, platformClient, kafkaAgentClient, context.nodeRef()), time);
            context.setRetryFlag(false);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, KafkaAgentClient kafkaAgentClient, NodeRef nodeRef) {
        State state;
        var nodeState = platformClient.podState(nodeRef);
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
                    BrokerState brokerState = kafkaAgentClient.getBrokerState(nodeRef.podName());
                    LOGGER.debugCr(reconciliation, "Pod {}: brokerState is {}", nodeRef, brokerState);

                    if (brokerState.isBrokerReady()) {
                        state = State.READY;
                    } else if (brokerState.isBrokerInRecovery()) {
                        LOGGER.warnCr(reconciliation, "Pod {} is not ready because the Kafka is performing log recovery. There are {} logs and {} segments left to recover", nodeRef, brokerState.remainingLogsToRecover(), brokerState.remainingSegmentsToRecover());
                        state = State.RECOVERING;
                    } else {
                        state = State.NOT_READY;
                    }
                } catch (Exception e) {
                    LOGGER.warnCr(reconciliation, "Could not get broker state for pod {}. This might be temporary if a pod was just restarted", nodeRef, e);
                    state = State.NOT_READY;
                }
        }
        LOGGER.debugCr(reconciliation, "Pod {}: observation outcome is {}", nodeRef, state);
        return state;
    }


    private long awaitReadyState(RestartContext restartContext, long timeoutMs) throws TimeoutException {
        LOGGER.infoCr(reconciliation, "Pod {}: Waiting for pod to enter state {}", restartContext.nodeRef(), State.READY);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + State.READY + " within " + timeoutMs + " ms: " + restartContext
        ).poll(pollingIntervalMs, () -> {
            var state = restartContext.transitionTo(observe(reconciliation, platformClient, kafkaAgentClient, restartContext.nodeRef()), time);
            return state == State.READY;
        });
    }


    private CompletableFuture<Void> waitForLogRecovery() {
        long remainingTimeoutMs = postOperationTimeoutMs;

        for (RestartContext context : restartContexts) {
            if (context.state() != State.READY && context.state() != State.NOT_RUNNING) {
                try {
                    remainingTimeoutMs = awaitReadyState(context, remainingTimeoutMs);
                } catch (TimeoutException e) {
                    if  (context.state() == State.RECOVERING) {
                        context.setRetryFlag(true);
                        //TODO: previously we included remaining logs and segments to recover in the error for the reconciliation but we log a warning for it during observe()
                        return CompletableFuture.failedFuture(new RetriableException("Pod " + context.nodeRef() + " is not ready because the Kafka is performing log recovery"));
                    } else {
                        LOGGER.warnCr(reconciliation, "Pod {} is not ready. We will check if KafkaRoller can do anything about it.", context.nodeRef().podName());
                    }
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> maybeForceRestartNodes() {
        List<RestartContext> controllersToRestartInParallel = new ArrayList<>();
        List<RestartContext> controllersToRestart = new ArrayList<>();
        List<RestartContext> brokersToRestart = new ArrayList<>();

        for (var c : restartContexts) {
            if (c.state().equals(State.NOT_RUNNING)) {
                if (c.reason().contains(RestartReason.POD_HAS_OLD_REVISION)) {
                    LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it seems to be stuck and restart might help", c.nodeRef());
                    if (c.currentRoles().controller()) {
                        controllersToRestartInParallel.add(c);
                    } else {
                        brokersToRestart.add(c);
                    }
                } else {
                    return CompletableFuture.failedFuture(new FatalException("Pod " + c.nodeRef().podName() + " is unschedulable or is not starting"));
                }
            } else if (!kafkaRollerClient.canConnectToNode(c.nodeRef(), c.currentRoles().broker())) {
                LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it does not seem to responding to connection attempts", c.nodeRef());
                if (c.currentRoles().controller()) {
                    controllersToRestart.add(c);
                } else {
                    brokersToRestart.add(c);
                }
            }
        }

        if (controllersToRestartInParallel.isEmpty() && controllersToRestart.isEmpty() && brokersToRestart.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> restartResult;
            if (!controllersToRestartInParallel.isEmpty()) {
                if (controllersToRestartInParallel.size() > 1) {
                    LOGGER.warnCr(reconciliation, "There are multiple controller pods that are not running, which runs a risk of losing the quorum. Restarting them in parallel: {}", controllersToRestartInParallel);
                }
                restartResult = restartInParallelAndAwaitReadiness(controllersToRestartInParallel);
            } else if (!controllersToRestart.isEmpty()) {
                RestartContext controller = controllersToRestart.getFirst();
                restartResult = restartPodAndAwaitReadiness(controller);
                controllersToRestart.remove(controller);
            } else {
                RestartContext broker = brokersToRestart.getFirst();
                restartResult = restartPodAndAwaitReadiness(broker);
                brokersToRestart.remove(broker);
            }

            return restartResult.thenCompose((i) -> {
                if (brokersToRestart.isEmpty() && controllersToRestart.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return CompletableFuture.failedFuture(new RetriableException("There are more pods that may still need to be force restarted: " + controllersToRestart + " " + brokersToRestart));
                }
            });
        }
    }

    private CompletableFuture<Void> restartPodAndAwaitReadiness(RestartContext restartContext) {
        try {
            restart(restartContext);
        } catch (Exception e) {
            restartContext.setRetryFlag(true);
            return CompletableFuture.failedFuture(new RetriableException("Error while trying to restart pod " + restartContext.nodeRef().podName() + " to become ready: " + e));
        }

        long remainingTimeoutMs = postOperationTimeoutMs;
        try {
            remainingTimeoutMs = awaitReadyState(restartContext, remainingTimeoutMs);
            if (restartContext.currentRoles().broker()) {
                awaitPreferred(restartContext, remainingTimeoutMs);
            }
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new FatalException("Error while waiting for restarted pod " + restartContext.nodeRef().podName() + " to become ready: " + e));
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> restartInParallelAndAwaitReadiness(List<RestartContext> batch) {
        for (RestartContext restartContext : batch) {
            try {
                restart(restartContext);
            } catch (Exception e) {
                restartContext.setRetryFlag(true);
                return CompletableFuture.failedFuture(new RetriableException("Error while trying to restart pod " + restartContext.nodeRef().podName() + " to become ready: " + e));
            }
        }

        long remainingTimeoutMs = postOperationTimeoutMs;
        for (RestartContext restartContext : batch) {
            try {
                remainingTimeoutMs = awaitReadyState(restartContext, remainingTimeoutMs);
                if (restartContext.currentRoles().broker()) {
                    awaitPreferred(restartContext, remainingTimeoutMs);
                }
            } catch (Exception e) {
                return CompletableFuture.failedFuture(new FatalException("Error while waiting for restarted pod " + restartContext.nodeRef().podName() + " to become ready: " + e));
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private void restart(RestartContext restartContext) {
        LOGGER.infoCr(reconciliation, "Pod {}: Restarting", restartContext.nodeRef());
        platformClient.restartPod(restartContext.nodeRef(), restartContext.reason());
        restartContext.transitionTo(State.UNKNOWN, time);
        restartContext.setRestarted(true);
        LOGGER.infoCr(reconciliation, "Pod {}: Restarted", restartContext.nodeRef());
    }

    private void awaitPreferred(RestartContext restartContext, long timeoutMs) {
        // TODO: apply configured delay (via env variable) before triggering leader election.
        //  This should be probably passed to tryElectAllPreferredLeaders so that delay is only applied
        //  if there are topic partitions to elect, otherwise no point of delaying the process
        time.sleep(10000L, 0);
        LOGGER.debugCr(reconciliation, "Pod {}: Waiting for Kafka broker to be leader of all its preferred replicas", restartContext.nodeRef());
        try {
            Alarm.timer(time,
                            timeoutMs,
                            () -> "Failed to elect the preferred leader " + restartContext.nodeRef() + " for topic partitions within " + timeoutMs)
                    .poll(pollingIntervalMs, () -> kafkaRollerClient.tryElectAllPreferredLeaders(restartContext.nodeRef()) == 0);
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for pod " + restartContext.nodeRef() + " to be leader for all its preferred replicas");
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Failed to elect preferred replica", e);
        }
    }

    private CompletableFuture<Void> reconfigureNodes() {
        if (allowReconfiguration) {
            for (RestartContext restartContext : restartContexts) {
                if (restartContext.reason().getReasons().isEmpty()) {
                    Config configs;

                    try {
                        if (restartContext.currentRoles().broker()) {
                            //TODO: create one admin call , if that fails, then make individual calls for the config
                            configs = kafkaRollerClient.describeBrokerConfigs(restartContext.nodeId());
                        } else {
                            configs = kafkaRollerClient.describeControllerConfigs(restartContext.nodeId());
                        }
                    } catch (Exception e) {
                        LOGGER.warnCr(reconciliation, "Error getting getting Kafka configs for {}.", restartContext.nodeRef());
                        restartContext.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                        continue;
                    }

                    var diff = new KafkaConfigurationDiff(reconciliation,
                            configs,
                            kafkaConfigProvider.apply(restartContext.nodeId()),
                            kafkaVersion,
                            restartContext.nodeRef(),
                            restartContext.currentRoles().controller(),
                            restartContext.currentRoles().broker());

                    if (!diff.isEmpty()) {
                        if (diff.canBeUpdatedDynamically()) {
                            try {
                                //TODO: can we do joint call or has to be done singular?
                                reconfigureNode(reconciliation, time, kafkaRollerClient, restartContext, diff);
                            } catch (Exception e) {
                                LOGGER.warnCr(reconciliation, "Failed to reconfigure {} due to {} therefore will restart", restartContext.nodeRef(), e);
                                restartContext.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                            }
                        } else {
                            restartContext.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                        }
                    }
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private static void reconfigureNode(Reconciliation reconciliation,
                                        Time time,
                                        KafkaRollerClient kafkaRollerClient,
                                        RestartContext restartContext,
                                        KafkaConfigurationDiff configDiff) {
        LOGGER.debugCr(reconciliation, "Pod {}: Reconfiguring", restartContext.nodeRef());
        kafkaRollerClient.reconfigureNode(restartContext.nodeRef(), configDiff, restartContext.currentRoles().broker());
        restartContext.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Pod {}: Reconfigured", restartContext.nodeRef());
    }

    private CompletableFuture<Void> restartControllers() {
        var orderedContexts = new ArrayList<>(restartContexts.stream()
                .filter(c -> !c.reason().getReasons().isEmpty()
                        && c.currentRoles().controller() && !c.isRestarted())
                .sorted(Comparator.comparing((RestartContext c) -> c.state().equals(State.READY))) // Sort by the roles (combined goes to the back)
                .toList());

        if (orderedContexts.isEmpty()) {
            LOGGER.debugCr(reconciliation, "There are no controller pods that need to be restarted");
            return CompletableFuture.completedFuture(null);
        }

        int activeControllerId;
        try {
            activeControllerId = kafkaRollerClient.getActiveControllerId();
        } catch (Exception e) {
            orderedContexts.forEach(c -> c.setRetryFlag(true));
            return CompletableFuture.failedFuture(new RetriableException("An error while trying to determine the active controller", e.getCause()));
        }
        LOGGER.debugCr(reconciliation, "Active controller is {}", activeControllerId);

        // Sort by leader (active leader goes to the back)
        orderedContexts.sort(Comparator.comparing((RestartContext c) -> c.nodeId() == activeControllerId));

        LOGGER.infoCr(reconciliation, "Attempting to restart controllers in the order: {}", orderedContexts);

        CompletableFuture<Void> restartResult = null;
        KafkaQuorumCheck2 quorumCheck = new KafkaQuorumCheck2(reconciliation, kafkaRollerClient);
        KafkaAvailability2 availability = new KafkaAvailability2(reconciliation, kafkaRollerClient);

        for (RestartContext context : orderedContexts) {
            if (canRollController(context, quorumCheck)) {

                // if combined mode, check availability as well
                if (context.currentRoles().broker()) {
                    if (canRollBroker(context, availability)) {
                        restartResult = restartPodAndAwaitReadiness(context);
                        break;
                    } else {
                        LOGGER.debugCr(reconciliation, "Pod {} cannot be restarted safely right now", context.nodeRef());
                        context.setRetryFlag(true);
                    }
                } else {
                    restartResult = restartPodAndAwaitReadiness(context);
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
                if (orderedContexts.size() > 1) {
                    return CompletableFuture.failedFuture(new RetriableException("There are more controller pods that may need restarting: " + orderedContexts));
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
        } else {
            return CompletableFuture.failedFuture(new RetriableException("None of the pods can be restarted safely " + orderedContexts));
        }
    }

    private boolean canRollController(RestartContext context, KafkaQuorumCheck2 quorumCheck) {
        try {
            return quorumCheck.canRollController(context.nodeId());
        } catch (Exception e) {
            // If we're not able to connect then roll
            if (e.getCause() instanceof SslAuthenticationException) {
                context.reason().add(RestartReason.POD_UNRESPONSIVE);
                return true;
            } else {
                LOGGER.warnCr(reconciliation, "Error checking if {} can roll", context.nodeRef(), e);
                return false;
            }
        }
    }

    private boolean canRollBroker(RestartContext context, KafkaAvailability2 availability) {
        try {
            return availability.canRoll(context.nodeId());
        } catch (Exception e) {
            // If we're not able to connect then roll
            if (e.getCause() instanceof SslAuthenticationException) {
                context.reason().add(RestartReason.POD_UNRESPONSIVE);
                return true;
            } else {
                LOGGER.warnCr(reconciliation, "An error while trying to determine the possibility of updating Kafka broker pod {}", context.nodeRef(), e);
                return false;
            }
        }
    }

    private CompletableFuture<Void> restartBrokers() {
        var orderedContexts = new ArrayList<>(restartContexts.stream()
                .filter(c -> !c.reason().getReasons().isEmpty()
                        && c.currentRoles().broker() && !c.isRestarted())
                .sorted(Comparator.comparing((RestartContext c) -> c.state().equals(State.READY)))// Sort by the state (ready goes to the back)
                .toList());

        LOGGER.infoCr(reconciliation, "Attempting to restart brokers in the order: {}", orderedContexts);

        if (orderedContexts.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        KafkaAvailability2 availability = new KafkaAvailability2(reconciliation, kafkaRollerClient);

        List<RestartContext> nodesToBatch = new ArrayList<>();
        for (RestartContext context : orderedContexts) {

            if (canRollBroker(context, availability)) {
                nodesToBatch.add(context);
            } else {
                context.setRetryFlag(true);
            }
        }

        if (nodesToBatch.isEmpty()) {
            return CompletableFuture.failedFuture(new RetriableException("Pods cannot be safely restarted: " + orderedContexts));
        }

        CompletableFuture<Void> restartResult;
        if (maxRestartBatchSize > 1) {
            // Instead of describing topic descriptions again, reuse the one we just got for the availability check
            var nextBatchBrokers = nextBatchBrokers(reconciliation, availability.getTopicDescriptions(), nodesToBatch, maxRestartBatchSize);
            restartResult = restartInParallelAndAwaitReadiness(nextBatchBrokers);
            orderedContexts.removeAll(nextBatchBrokers);
        } else {
            RestartContext nextBroker = nodesToBatch.getFirst();
            restartResult = restartPodAndAwaitReadiness(nextBroker);
            orderedContexts.remove(nextBroker);
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
    private List<RestartContext> nextBatchBrokers(Reconciliation reconciliation,
                                                  Collection<TopicDescription> topicDescriptions,
                                                  List<RestartContext> brokersNeedingRestart,
                                                  int maxRestartBatchSize) {
        LOGGER.debugCr(reconciliation, "Parallel batching of broker pods is enabled. Max batch size is {}", maxRestartBatchSize);

        Batching batching = new Batching(reconciliation, topicDescriptions);

        var batchToRestart = batching.getBatchedBrokersToRestart(brokersNeedingRestart.stream().map(KafkaRoller2.RestartContext::nodeId).collect(Collectors.toSet()), maxRestartBatchSize);

        return brokersNeedingRestart.stream().filter(c -> batchToRestart.contains(c.nodeId())).toList();
    }

    /**
     * Enumerates the possible "rolling states" of a Kafka node
     */
    enum State {
        UNKNOWN, // the initial state
        NOT_RUNNING, // The pod/process is not running.
        NOT_READY, // decided to restart right now or broker state < 2 OR == 127
        RECOVERING, // broker state == 2
        READY, // broker state >= 3 AND != 127
    }

    /**
     * Per-server context information during a rolling restart/reconfigure
     */
    public static final class RestartContext {
        /** The node this context refers to */
        private final NodeRef nodeRef;
        /** The process roles currently assigned to the node */
        private final KafkaNodeRoles currentRoles;
        /** The state of the node the last time it was observed */
        private State state;
        /** The time of the last state transition */
        private long lastTransition;
        /** The reasons this node needs to be restarted or reconfigured */
        private final RestartReasons reason;
        /** The number of operational attempts so far. */
        private final BackOff backOff;
        /** Whether it needs to be reattempted for a restart or reconfiguration */
        private boolean shouldRetry;
        /** Whether it has been already restarted in this reconciliation */
        private boolean restarted;

        private RestartContext(NodeRef nodeRef, KafkaNodeRoles currentRoles, State state, long lastTransition, RestartReasons reason, BackOff backOff) {
            this.nodeRef = nodeRef;
            this.currentRoles = currentRoles;
            this.state = state;
            this.lastTransition = lastTransition;
            this.reason = reason;
            this.backOff = backOff;
        }

        static RestartContext start(NodeRef nodeRef,
                                    KafkaNodeRoles nodeRoles,
                                    Function<Pod, RestartReasons> predicate,
                                    Supplier<BackOff> backOffSupplier,
                                    PodOperator podOperator,
                                    String namespace,
                                    Time time) {
            Pod pod = podOperator.get(namespace, nodeRef.podName());
            if (pod == null) {
                return new RestartContext(nodeRef, nodeRoles, State.UNKNOWN, time.systemTimeMillis(),  RestartReasons.empty(), backOffSupplier.get());
            } else {
                BackOff backOff = backOffSupplier.get();
                backOff.delayMs();
                return new RestartContext(nodeRef, nodeRoles, State.UNKNOWN, time.systemTimeMillis(), predicate.apply(pod), backOff);
            }
        }

        State transitionTo(State state, Time time) {
            if (this.state() == state) {
                return state;
            }
            this.state = state;

            this.lastTransition = time.systemTimeMillis();
            return state;
        }

        private NodeRef nodeRef() {
            return nodeRef;
        }

        private int nodeId() {
            return nodeRef.nodeId();
        }

        private KafkaNodeRoles currentRoles() {
            return currentRoles;
        }

        private State state() {
            return state;
        }

        private long lastTransition() {
            return lastTransition;
        }

        private RestartReasons reason() {
            return reason;
        }

        private BackOff backOff() {
            return backOff;
        }

        private void setRetryFlag(boolean shouldRetry) {
            this.shouldRetry = shouldRetry;
        }

        private boolean shouldRetry() {
            return shouldRetry;
        }

        private void setRestarted(boolean restarted) {
            this.restarted = restarted;
        }

        private boolean isRestarted() {
            return restarted;
        }


        @Override
        public String toString() {
            return "Context[" +
                    "nodeRef=" + nodeRef + ", " +
                    "currentRoles=" + currentRoles + ", " +
                    "state=" + state + ", " +
                    "lastTransition=" + Instant.ofEpochMilli(lastTransition) + ", " +
                    "reason=" + reason + ']';
        }
    }

    static class RetriableException extends RuntimeException {

        /**
         * This exception indicates that KafkaRoller will re-attempt this node to either bring it to a healthy state, reconfigure or restart
         * @param message the detail message. The detail message is saved for later retrieval by the getMessage() method
         */
        public RetriableException(String message) {
            super(message);
        }

        RetriableException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }

    static class FatalException extends RuntimeException {

        /**
         * This exception indicates that KafkaRoller cannot re-attempt this node to bring it to a healthy state, reconfigure or restart
         * @param message the detail message. The detail message is saved for later retrieval by the getMessage() method
         */
        public FatalException(String message) {
            super(message);
        }

    }
}