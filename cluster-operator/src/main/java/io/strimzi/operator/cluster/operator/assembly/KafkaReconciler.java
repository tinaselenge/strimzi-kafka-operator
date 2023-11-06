/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.UsedNodePoolStatus;
import io.strimzi.api.kafka.model.status.UsedNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.model.NodeUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.cluster.model.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;

/**
 * Class used for reconciliation of Kafka. This class contains both the steps of the Kafka
 * reconciliation pipeline (although the steps for listener reconciliation are outsourced to the KafkaListenerReconciler)
 * and is also used to store the state between them.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    /* test */ final Reconciliation reconciliation;
    private final Vertx vertx;
    private final long operationTimeoutMs;
    /* test */ final KafkaCluster kafka;
    private final List<KafkaNodePool> kafkaNodePoolCrs;
    private final ClusterCa clusterCa;
    private final ClientsCa clientsCa;
    private final List<String> maintenanceWindows;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isKafkaNodePoolsEnabled;
    /* test */ final PlatformFeaturesAvailability pfa;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;

    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    /* test */ final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    /* test */ final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final PreventBrokerScaleDownCheck brokerScaleDownOperations;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodOperator podOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;
    /* test */ final RouteOperator routeOperator;
    /* test */ final IngressOperator ingressOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> kafkaNodePoolOperator;

    private final KubernetesRestartEventPublisher eventsPublisher;

    private final AdminClientProvider adminClientProvider;

    private final Set<String> fsResizingRestartRequest = new HashSet<>();
    private String logging = "";
    private String loggingHash = "";
    private final boolean skipBrokerScaleDownCheck;
    private final Map<Integer, String> brokerConfigurationHash = new HashMap<>();
    private final Map<Integer, String> kafkaServerCertificateHash = new HashMap<>();

    // Result of the listener reconciliation with the listener details
    /* test */ KafkaListenersReconciler.ReconciliationResult listenerReconciliationResults;

    /**
     * Constructs the Kafka reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafkaCr                   The Kafka custom resource
     * @param nodePools                 List of KafkaNodePool resources belonging to this cluster
     * @param oldStorage                Maps with old storage configurations, where the key is the name of the controller
     *                                  resource (e.g. my-cluster-pool-a) and the value is the current storage configuration.
     * @param currentPods               Map with current pods, where the key is the name of the controller resource
     *                                  (e.g. my-cluster-pool-a) and the value is a list with Pod names
     * @param clusterCa                 The Cluster CA instance
     * @param clientsCa                 The Clients CA instance
     * @param versionChange             Description of Kafka upgrade / downgrade state
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param vertx                     Vert.x instance
     */
    public KafkaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            ClusterCa clusterCa,
            ClientsCa clientsCa,
            KafkaVersionChange versionChange,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PlatformFeaturesAvailability pfa,
            Vertx vertx
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaNodePoolCrs = nodePools;

        boolean isKRaftEnabled = config.featureGates().useKRaftEnabled() && ReconcilerUtils.kraftEnabled(kafkaCr);
        // We prepare the KafkaPool models and create the KafkaCluster model
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(reconciliation, kafkaCr, nodePools, oldStorage, currentPods, isKRaftEnabled, supplier.sharedEnvironmentProvider);
        String clusterId = isKRaftEnabled ? NodePoolUtils.getOrGenerateKRaftClusterId(kafkaCr, nodePools) : NodePoolUtils.getClusterIdIfSet(kafkaCr, nodePools);
        this.kafka = KafkaCluster.fromCrd(reconciliation, kafkaCr, pools, config.versions(), isKRaftEnabled, clusterId, supplier.sharedEnvironmentProvider);

        // We set the user-configured inter.broker.protocol.version if needed (when not set by the user)
        // It is set only in ZooKeeper-mode since in Kraft mode it is ignored and throws warnings
        if (!isKRaftEnabled && versionChange.interBrokerProtocolVersion() != null) {
            this.kafka.setInterBrokerProtocolVersion(versionChange.interBrokerProtocolVersion());
        }

        // We set the user-configured log.message.format.version if needed (when not set by the user)
        // It is set only in ZooKeeper-mode since in Kraft mode it is ignored and throws warnings
        if (!isKRaftEnabled && versionChange.logMessageFormatVersion() != null) {
            this.kafka.setLogMessageFormatVersion(versionChange.logMessageFormatVersion());
        }

        this.clusterCa = clusterCa;
        this.clientsCa = clientsCa;
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.isKafkaNodePoolsEnabled = config.featureGates().kafkaNodePoolsEnabled() && ReconcilerUtils.nodePoolsEnabled(kafkaCr);
        this.pfa = pfa;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.skipBrokerScaleDownCheck = Annotations.booleanAnnotation(kafkaCr, Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, false);

        this.stsOperator = supplier.stsOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.brokerScaleDownOperations = supplier.brokerScaleDownOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.pvcOperator = supplier.pvcOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.podOperator = supplier.podOperations;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
        this.routeOperator = supplier.routeOperations;
        this.ingressOperator = supplier.ingressOperations;
        this.nodeOperator = supplier.nodeOperator;
        this.kafkaNodePoolOperator = supplier.kafkaNodePoolOperator;
        this.eventsPublisher = supplier.restartEventsPublisher;

        this.adminClientProvider = supplier.adminClientProvider;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param kafkaStatus   The Kafka Status class for adding conditions to it during the reconciliation
     * @param clock         The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                      That time is used for checking maintenance windows
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
        return modelWarnings(kafkaStatus)
                .compose(i -> brokerScaleDownCheck())
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> manualRollingUpdate())
                .compose(i -> pvcs(kafkaStatus))
                .compose(i -> serviceAccount())
                .compose(i -> initClusterRoleBinding())
                .compose(i -> scaleDown())
                .compose(i -> updateNodePoolStatuses(kafkaStatus))
                .compose(i -> listeners())
                .compose(i -> certificateSecret(clock))
                .compose(i -> brokerConfigurationConfigMaps())
                .compose(i -> jmxSecret())
                .compose(i -> podDisruptionBudget())
                .compose(i -> migrateFromStatefulSetToPodSet())
                .compose(i -> podSet())
                .compose(podSetDiffs -> rollingUpdate(podSetDiffs)) // We pass the PodSet reconciliation result this way to avoid storing it in the instance
                .compose(i -> podsReady())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> clusterId(kafkaStatus))
                .compose(i -> deletePersistentClaims())
                .compose(i -> sharedKafkaConfigurationCleanup())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(i -> nodePortExternalListenerStatus())
                .compose(i -> addListenersToKafkaStatus(kafkaStatus))
                .compose(i -> updateKafkaVersion(kafkaStatus));
    }

    protected Future<Void> brokerScaleDownCheck() {
        if (skipBrokerScaleDownCheck || kafka.removedNodes().isEmpty()) {
            return Future.succeededFuture();
        } else {
            return brokerScaleDownOperations.canScaleDownBrokers(reconciliation, vertx, kafka.removedNodes(), secretOperator, adminClientProvider)
                    .compose(brokersContainingPartitions -> {
                        if (!brokersContainingPartitions.isEmpty()) {
                            throw new InvalidResourceException("Cannot scale down brokers " + kafka.removedNodes() + " because brokers " + brokersContainingPartitions + " are not empty");
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }
    }


    /**
     * Takes the warning conditions from the Model and adds them in the KafkaStatus
     *
     * @param kafkaStatus   The Kafka Status where the warning conditions will be added
     *
     * @return              Completes when the warnings are added to the status object
     */
    protected Future<Void> modelWarnings(KafkaStatus kafkaStatus) {
        List<Condition> conditions = kafka.getWarningConditions();

        kafkaStatus.addConditions(conditions);

        return Future.succeededFuture();
    }

    /**
     * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                kafka.getSelectorLabels(),
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning();
    }

    /**
     * Manages the network policy protecting the Kafka cluster
     *
     * @return  Completes when the network policy is successfully created or updated
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaNetworkPolicyName(reconciliation.name()), kafka.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels))
                    .map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Does manual rolling update of Kafka pods based on an annotation on the StrimziPodSet or on the Pods. Annotation
     * on StrimziPodSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StrimziPodSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        Future<List<NodeRef>> podsToRollThroughPodSetAnno = podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation();
        Future<List<NodeRef>> podsToRollThroughPodAnno = podsForManualRollingUpdateDiscoveredThroughPodAnnotations();

        return Future
                .join(podsToRollThroughPodSetAnno, podsToRollThroughPodAnno)
                .compose(result -> {
                    // We merge the lists into set to avoid duplicates
                    Set<NodeRef> nodes = new LinkedHashSet<>();
                    nodes.addAll(result.resultAt(0));
                    nodes.addAll(result.resultAt(1));

                    if (!nodes.isEmpty())   {
                        return maybeRollKafka(
                                nodes,
                                pod -> {
                                    if (pod == null) {
                                        throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StrimziPodSet.");
                                    }

                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation", pod.getMetadata().getName());

                                    return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                                },
                                // Pass empty advertised hostnames and ports for the nodes
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                false
                        );
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Checks all Kafka PodSets and if they have the manual rolling update annotation, it will take all their nodes and
     * add them to a list for rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation()   {
        return strimziPodSetOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(podSets -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (StrimziPodSet podSet : podSets) {
                        if (Annotations.booleanAnnotation(podSet, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.addAll(ReconcilerUtils.nodesFromPodSet(podSet));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on their StrimziPodSet", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Checks all Kafka Pods and if they have the manual rolling update annotation, it will add them to a list for
     * rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodAnnotations()   {
        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(pods -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (Pod pod : pods) {
                        if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.add(ReconcilerUtils.nodeFromPod(pod));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on the Pod", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Rolls Kafka pods if needed
     *
     * @param nodes                     List of nodes which should be considered for rolling
     * @param podNeedsRestart           Function which serves as a predicate whether to roll pod or not
     * @param kafkaAdvertisedHostnames  Map with advertised hostnames required to generate the per-broker configuration
     * @param kafkaAdvertisedPorts      Map with advertised ports required to generate the per-broker configuration
     * @param allowReconfiguration      Defines whether the rolling update should also attempt to do dynamic reconfiguration or not
     *
     * @return  Future which completes when the rolling is complete
     */
    protected Future<Void> maybeRollKafka(
            Set<NodeRef> nodes,
            Function<Pod, RestartReasons> podNeedsRestart,
            Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
            Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
            boolean allowReconfiguration
    ) {
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture ->
                        new KafkaRoller(
                                reconciliation,
                                vertx,
                                podOperator,
                                1_000,
                                operationTimeoutMs,
                                () -> new BackOff(250, 2, 10),
                                nodes,
                                compositeFuture.resultAt(0),
                                compositeFuture.resultAt(1),
                                adminClientProvider,
                                brokerId -> kafka.generatePerBrokerConfiguration(brokerId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts),
                                logging,
                                kafka.getKafkaVersion(),
                                allowReconfiguration,
                                eventsPublisher
                        ).rollingRestart(podNeedsRestart));
    }

    /**
     * Manages the PVCs needed by the Kafka cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @param kafkaStatus   Status of the Kafka custom resource where warnings about any issues with resizing will be added
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs(KafkaStatus kafkaStatus) {
        List<PersistentVolumeClaim> pvcs = kafka.generatePersistentVolumeClaims();

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(kafkaStatus, podIndex -> KafkaResources.kafkaPodName(reconciliation.name(), podIndex), pvcs)
                .compose(podsToRestart -> {
                    fsResizingRestartRequest.addAll(podsToRestart);
                    return Future.succeededFuture();
                });
    }

    /**
     * Manages the Kafka service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.generateServiceAccount())
                .map((Void) null);
    }

    /**
     * Manages the Kafka cluster role binding. When the desired Cluster Role Binding is null, and we get an RBAC error,
     * we ignore it. This is to allow users to run the operator only inside a namespace when no features requiring
     * Cluster Role Bindings are needed.
     *
     * @return  Completes when the Cluster Role Binding was successfully created or updated
     */
    protected Future<Void> initClusterRoleBinding() {
        ClusterRoleBinding desired = kafka.generateClusterRoleBinding(reconciliation.namespace());

        return ReconcilerUtils.withIgnoreRbacError(
                reconciliation,
                clusterRoleBindingOperator
                        .reconcile(
                                reconciliation,
                                KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()),
                                desired
                        ),
                desired
        ).map((Void) null);
    }

    /**
     * Scales down the Kafka cluster if needed. Kafka scale-down is done in one go.
     *
     * @return  Future which completes when the scale-down is finished
     */
    protected Future<Void> scaleDown() {
        LOGGER.debugCr(reconciliation, "Checking if Kafka scale-down is needed");

        Set<String> desiredPodNames = new HashSet<>();
        for (NodeRef node : kafka.nodes()) {
            desiredPodNames.add(node.podName());
        }

        return strimziPodSetOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(podSets -> {
                    if (podSets == null) {
                        return Future.succeededFuture();
                    } else {
                        List<Future<Void>> ops = new ArrayList<>();

                        for (StrimziPodSet podSet : podSets) {
                            List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                    .filter(pod -> desiredPodNames.contains(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                                    .collect(Collectors.toList());

                            StrimziPodSet scaledDownPodSet = new StrimziPodSetBuilder(podSet)
                                    .editSpec()
                                    .withPods(desiredPods)
                                    .endSpec()
                                    .build();

                            if (podSet.getSpec().getPods().size() > desiredPods.size())    {
                                LOGGER.infoCr(reconciliation, "Scaling down Kafka pod set {} from {} to {} replicas", podSet.getMetadata().getName(), podSet.getSpec().getPods().size(), desiredPods.size());
                                ops.add(
                                        strimziPodSetOperator
                                                .reconcile(reconciliation, reconciliation.namespace(), podSet.getMetadata().getName(), scaledDownPodSet)
                                                .map((Void) null)
                                );
                            }
                        }

                        return Future.join(ops).map((Void) null);
                    }
                });
    }

    /**
     * Utility method to create the Kafka Listener reconciler. It can be also use to inject mocked reconciler during
     * tests.
     *
     * @return  KafkaListenerReconciler instance
     */
    protected KafkaListenersReconciler listenerReconciler()   {
        return new KafkaListenersReconciler(
                reconciliation,
                kafka,
                clusterCa,
                pfa,
                operationTimeoutMs,
                secretOperator,
                serviceOperator,
                routeOperator,
                ingressOperator
        );
    }

    /**
     * Reconciles listeners of this Kafka cluster
     *
     * @return  Future which completes when listeners are reconciled
     */
    protected Future<Void> listeners()    {
        return listenerReconciler()
                .reconcile()
                .compose(result -> {
                    listenerReconciliationResults = result;
                    return Future.succeededFuture();
                });
    }

    /**
     * Generates and creates the ConfigMaps with per-broker configuration for Kafka brokers used in PodSets. It will
     * also delete the ConfigMaps for any scaled-down brokers (scale down is done before this is called in the
     * reconciliation)
     *
     * @param metricsAndLogging     Metrics and Logging configuration
     *
     * @return  Future which completes when the Kafka Configuration is prepared
     */
    protected Future<Void> perBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        return configMapOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(existingConfigMaps -> {
                    // This is used during Kafka rolling updates -> we have to store it for later
                    this.logging = kafka.logging().loggingConfiguration(reconciliation, metricsAndLogging.loggingCm());
                    this.loggingHash = Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging));

                    List<ConfigMap> desiredConfigMaps = kafka.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, listenerReconciliationResults.advertisedHostnames, listenerReconciliationResults.advertisedPorts);
                    List<Future<?>> ops = new ArrayList<>();

                    // Delete all existing ConfigMaps which are not desired and are not the shared config map
                    List<String> desiredNames = new ArrayList<>(desiredConfigMaps.size() + 1);
                    desiredNames.add(KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name())); // We do not want to delete the shared ConfigMap, so we add it here
                    desiredNames.addAll(desiredConfigMaps.stream().map(cm -> cm.getMetadata().getName()).toList());

                    for (ConfigMap cm : existingConfigMaps) {
                        // We delete the cms not on the desired names list
                        if (!desiredNames.contains(cm.getMetadata().getName())) {
                            ops.add(configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                        }
                    }

                    // Create / update the desired config maps
                    for (ConfigMap cm : desiredConfigMaps) {
                        String cmName = cm.getMetadata().getName();
                        int nodeId = ReconcilerUtils.getPodIndexFromPodName(cmName);

                        // The advertised hostname and port might change. If they change, we need to roll the pods.
                        // Here we collect their hash to trigger the rolling update. For per-broker configuration,
                        // we need just the advertised hostnames / ports for given broker.
                        String brokerConfiguration = listenerReconciliationResults.advertisedHostnames
                                .get(nodeId)
                                .entrySet()
                                .stream()
                                .map(kv -> kv.getKey() + "://" + kv.getValue())
                                .sorted()
                                .collect(Collectors.joining(" "));
                        brokerConfiguration += listenerReconciliationResults.advertisedPorts
                                .get(nodeId)
                                .entrySet()
                                .stream()
                                .map(kv -> kv.getKey() + "://" + kv.getValue())
                                .sorted()
                                .collect(Collectors.joining(" "));
                        brokerConfiguration += cm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");

                        // Changes to regular Kafka configuration are handled through the KafkaRoller which decides whether to roll the pod or not
                        // In addition to that, we have to handle changes to configuration unknown to Kafka -> different plugins (Authorization, Quotas etc.)
                        // This is captured here with the unknown configurations and the hash is used to roll the pod when it changes
                        KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, cm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));

                        String controllerConfiguration = "";
                        KafkaPool pool = kafka.nodePoolForNodeId(nodeId);
                        if (pool.isController() && !pool.isBroker())   {
                            // For controllers only, we extract the controller-relevant configurations and use it in the configuration annotations
                            controllerConfiguration = kc.controllerConfigsWithValues().toString();
                        }

                        // We store hash of the broker configurations for later use in Pod and in rolling updates
                        this.brokerConfigurationHash.put(nodeId, Util.hashStub(brokerConfiguration + kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString() + controllerConfiguration));

                        ops.add(configMapOperator.reconcile(reconciliation, reconciliation.namespace(), cmName, cm));
                    }

                    return Future
                            .join(ops)
                            .map((Void) null);
                });
    }

    /**
     * This method is used to create or update the config maps required by the brokers. It does not do the cleanup the
     * old shared Config Map used by StatefulSets. That is done only at the end of the reconciliation. However, it would
     * delete the config maps of the scaled-down brokers since scale-down happens before this is called.
     *
     * @return  Future which completes when the Config Map(s) with configuration are created or updated
     */
    protected Future<Void> brokerConfigurationConfigMaps() {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, kafka.logging(), kafka.metrics())
                .compose(metricsAndLoggingCm -> perBrokerKafkaConfiguration(metricsAndLoggingCm));
    }

    /**
     * Manages the Secret with the node certificates used by the Kafka brokers.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Completes when the Secret was successfully created or updated
     */
    protected Future<Void> certificateSecret(Clock clock) {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaSecretName(reconciliation.name()))
                .compose(oldSecret -> {
                    return secretOperator
                            .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaSecretName(reconciliation.name()),
                                    kafka.generateCertificatesSecret(clusterCa, clientsCa, listenerReconciliationResults.bootstrapDnsNames, listenerReconciliationResults.brokerDnsNames, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())))
                            .compose(patchResult -> {
                                if (patchResult != null) {
                                    for (NodeRef node : kafka.nodes()) {
                                        kafkaServerCertificateHash.put(
                                                node.nodeId(),
                                                CertUtils.getCertificateThumbprint(patchResult.resource(),
                                                        ClusterCa.secretEntryNameForPod(node.podName(), Ca.SecretEntry.CRT)
                                                ));
                                    }
                                }

                                return Future.succeededFuture();
                            });
                });
    }

    /**
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperator, kafka);
    }

    /**
     * Manages the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully created or updated
     */
    protected Future<Void> podDisruptionBudget() {
        return podDisruptionBudgetOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), kafka.generatePodDisruptionBudget())
                    .map((Void) null);
    }

    /**
     * Prepares annotations for Kafka pods within a StrimziPodSet which are known only in the KafkaAssemblyOperator level.
     * These are later passed to KafkaCluster where there are used when creating the Pod definitions.
     *
     * @param nodeId    ID of the broker, the annotations of which are being prepared.
     *
     * @return  Map with Pod annotations
     */
    private Map<String, String> podSetPodAnnotations(int nodeId) {
        Map<String, String> podAnnotations = new LinkedHashMap<>(9);
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clusterCa)));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(ModelUtils.caKeyGeneration(this.clusterCa)));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(ModelUtils.caCertGeneration(this.clientsCa)));
        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, loggingHash);
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, brokerConfigurationHash.get(nodeId));
        podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafka.getKafkaVersion().version());

        String logMessageFormatVersion = kafka.getLogMessageFormatVersion();
        if (logMessageFormatVersion != null && !logMessageFormatVersion.isBlank()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
        }

        String interBrokerProtocolVersion = kafka.getInterBrokerProtocolVersion();
        if (interBrokerProtocolVersion != null && !interBrokerProtocolVersion.isBlank()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
        }

        podAnnotations.put(ANNO_STRIMZI_SERVER_CERT_HASH, kafkaServerCertificateHash.get(nodeId)); // Annotation of broker certificate hash

        // Annotations with custom cert thumbprints to help with rolling updates when they change
        if (!listenerReconciliationResults.customListenerCertificateThumbprints.isEmpty()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, listenerReconciliationResults.customListenerCertificateThumbprints.toString());
        }

        return podAnnotations;
    }

    /**
     * Helps with the migration from StatefulSets to StrimziPodSets when the cluster is switching between them. When the
     * switch happens, it deletes the old StatefulSet. It should happen before the new PodSet is created to
     * allow the controller hand-off.
     *
     * @return          Future which completes when the StatefulSet is deleted or does not need to be deleted
     */
    protected Future<Void> migrateFromStatefulSetToPodSet() {
        // Deletes the StatefulSet if it exists as a part of migration to PodSets
        return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                .compose(sts -> {
                    if (sts != null)    {
                        return stsOperator.deleteAsync(reconciliation, reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()), false);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Create or update the StrimziPodSet for the Kafka cluster. If set, it uses the old replica count since scaling-up
     * happens only later in a separate step.
     *
     * @return  Future which completes when the PodSet is created, updated or deleted
     */
    protected Future<Map<String, ReconcileResult<StrimziPodSet>>> podSet() {
        return strimziPodSetOperator
                .batchReconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        kafka.generatePodSets(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations),
                        kafka.getSelectorLabels()
                );
    }

    /**
     * Roles the Kafka brokers (if needed).
     *
     * @param podSetDiffs   Map with the PodSet reconciliation results
     *
     * @return  Future which completes when any of the Kafka pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate(Map<String, ReconcileResult<StrimziPodSet>> podSetDiffs) {
        return maybeRollKafka(
                kafka.nodes(),
                pod -> ReconcilerUtils.reasonsToRestartPod(
                        reconciliation,
                        podSetDiffs.get(ReconcilerUtils.getControllerNameFromPodName(pod.getMetadata().getName())).resource(),
                        pod,
                        fsResizingRestartRequest,
                        ReconcilerUtils.trackedServerCertChanged(pod, kafkaServerCertificateHash),
                        clusterCa,
                        clientsCa
                ),
                listenerReconciliationResults.advertisedHostnames,
                listenerReconciliationResults.advertisedPorts,
                true
        );
    }

    /**
     * Checks whether the Kafka pods are ready and if not, waits for them to get ready
     *
     * @return  Future which completes when all Kafka pods are ready
     */
    protected Future<Void> podsReady() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        kafka.nodes().stream().map(node -> node.podName()).toList()
                );
    }

    /**
     * Waits for readiness of the endpoints of the clients service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> serviceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.bootstrapServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Waits for readiness of the endpoints of the headless service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> headlessServiceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Get the Cluster ID of the Kafka cluster
     *
     * @return  Future which completes when the Cluster ID is retrieved and set in the status
     */
    protected Future<Void> clusterId(KafkaStatus kafkaStatus) {
        return ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
                    return vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                            .executeBlocking(() -> {
                                Admin kafkaAdmin = null;

                                try {
                                    String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                                    LOGGER.debugCr(reconciliation, "Creating AdminClient for clusterId using {}", bootstrapHostname);
                                    kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");
                                    kafkaStatus.setClusterId(kafkaAdmin.describeCluster().clusterId().get());
                                } catch (KafkaException e) {
                                    LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                                } catch (InterruptedException e) {
                                    LOGGER.warnCr(reconciliation, "Interrupted exception getting clusterId {}", e.getMessage());
                                } catch (ExecutionException e) {
                                    LOGGER.warnCr(reconciliation, "Execution exception getting clusterId {}", e.getMessage());
                                } finally {
                                    if (kafkaAdmin != null) {
                                        kafkaAdmin.close();
                                    }
                                }

                                return null;
                            });
                });
    }

    /**
     * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     *
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> deletePersistentClaims() {
        return pvcOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafka.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }

    /**
     * Deletes the ConfigMap with shared Kafka configuration. This needs to be done after migrating from StatefulSets to StrimziPodSets
     *
     * @return  Future which returns when the shared configuration config map is deleted
     */
    protected Future<Void> sharedKafkaConfigurationCleanup() {
        // We use reconcile() instead of deleteAsync() because reconcile first checks if the deletion is needed.
        // Deleting resource which likely does not exist would cause more load on the Kubernetes API then trying to get
        // it first because of the watch if it was deleted etc.
        return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()), null)
                .map((Void) null);
    }

    /**
     * Creates the status for NodePort listeners. This can be done only now at the end and not when the other listener
     * types are done because it requires the Kafka brokers to be scheduled and running to collect their node addresses.
     * Without that, we do not know on which node would they be running.
     *
     * @return  Future which completes when the Listener status is created for all node port listeners
     */
    protected Future<Void> nodePortExternalListenerStatus() {
        List<Node> allNodes = new ArrayList<>();

        if (!ListenersUtils.nodePortListeners(kafka.getListeners()).isEmpty())   {
            return nodeOperator.listAsync(Labels.EMPTY)
                    .compose(result -> {
                        allNodes.addAll(result);
                        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels());
                    })
                    .map(pods -> {
                        Map<Integer, Node> brokerNodes = new HashMap<>();

                        for (Pod broker : pods) {
                            String podName = broker.getMetadata().getName();
                            Integer podIndex = ReconcilerUtils.getPodIndexFromPodName(podName);

                            if (broker.getStatus() != null && broker.getStatus().getHostIP() != null) {
                                String hostIP = broker.getStatus().getHostIP();
                                allNodes.stream()
                                        .filter(node -> {
                                            if (node.getStatus() != null && node.getStatus().getAddresses() != null) {
                                                return node.getStatus().getAddresses().stream().anyMatch(address -> hostIP.equals(address.getAddress()));
                                            } else {
                                                return false;
                                            }
                                        })
                                        .findFirst()
                                        .ifPresent(podNode -> brokerNodes.put(podIndex, podNode));
                            }
                        }

                        for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafka.getListeners())) {
                            // Set is used to ensure each node/port is listed only once. It is later converted to List.
                            Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                            for (Map.Entry<Integer, Node> entry : brokerNodes.entrySet())   {
                                String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, entry.getKey());
                                ListenerAddress address;

                                if (advertisedHost != null)    {
                                    address = new ListenerAddressBuilder()
                                            .withHost(advertisedHost)
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else {
                                    address = new ListenerAddressBuilder()
                                            .withHost(NodeUtils.findAddress(entry.getValue().getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                }

                                statusAddresses.add(address);
                            }

                            ListenerStatus ls = listenerReconciliationResults.listenerStatuses
                                    .stream()
                                    .filter(listenerStatus -> listener.getName().equals(listenerStatus.getName()))
                                    .findFirst()
                                    .orElseThrow(() -> new RuntimeException("Status for listener " + listener.getName() + " not found"));
                            ls.setAddresses(new ArrayList<>(statusAddresses));
                        }

                        return null;
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    // Adds prepared Listener Statuses to the Kafka Status instance
    protected Future<Void> addListenersToKafkaStatus(KafkaStatus kafkaStatus) {
        kafkaStatus.setListeners(listenerReconciliationResults.listenerStatuses);
        return Future.succeededFuture();
    }

    // Adds Kafka version to the Kafka Status instance
    protected Future<Void> updateKafkaVersion(KafkaStatus kafkaStatus) {
        kafkaStatus.setKafkaVersion(kafka.getKafkaVersion().version());
        return Future.succeededFuture();
    }

    /**
     * Updates the statuses of the used KafkaNodePools with the used node IDs. Also prepares the list of used node pools
     * for the Kafka CR status (but the Kafka status is not updated in this method).
     *
     * @param kafkaStatus   The status of the Kafka CR to add the list of node pools belonging to it
     *
     * @return  Future which completes when the statuses are set
     */
    protected Future<Void> updateNodePoolStatuses(KafkaStatus kafkaStatus) {
        if (isKafkaNodePoolsEnabled) {
            List<KafkaNodePool> updatedNodePools = new ArrayList<>();
            List<UsedNodePoolStatus> statusesForKafka = new ArrayList<>();
            Map<String, KafkaNodePoolStatus> statuses = kafka.nodePoolStatuses();

            for (KafkaNodePool nodePool : kafkaNodePoolCrs) {
                statusesForKafka.add(new UsedNodePoolStatusBuilder().withName(nodePool.getMetadata().getName()).build());

                KafkaNodePool updatedNodePool = new KafkaNodePoolBuilder(nodePool)
                        .withStatus(
                                new KafkaNodePoolStatusBuilder(statuses.get(nodePool.getMetadata().getName()))
                                        .withObservedGeneration(nodePool.getMetadata().getGeneration())
                                        .build())
                        .build();

                StatusDiff diff = new StatusDiff(nodePool.getStatus(), updatedNodePool.getStatus());

                if (!diff.isEmpty()) {
                    // Status changed => we will update it
                    updatedNodePools.add(updatedNodePool);
                }
            }

            // Sets the list of used Node Pools in the Kafka CR status
            kafkaStatus.setKafkaNodePools(statusesForKafka.stream().sorted(Comparator.comparing(UsedNodePoolStatus::getName)).toList());

            List<Future<KafkaNodePool>> statusUpdateFutures = new ArrayList<>();

            for (KafkaNodePool updatedNodePool : updatedNodePools) {
                statusUpdateFutures.add(kafkaNodePoolOperator.updateStatusAsync(reconciliation, updatedNodePool));
            }

            // Return future
            return Future.join(statusUpdateFutures)
                    .map((Void) null);
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * This is used to get the Storage configuration used by the Kafka nodes. The storage configuration is needed by
     * Cruise Control. But collecting it directly in Cruise Control from the custom resources would be complicated as
     * it would need to figure out the node pools and the node IDs belonging to them. In addition, the storage
     * configuration might change (in case of un-allowed changes), but due to the possibility of illegal storage changes
     * which will be reverted it in the Kafka and Node Pool models.
     *
     * @return  Map with the pool names as the keys and storage configuration for given pools as the values
     */
    public Map<String, Storage> kafkaStorage()   {
        return kafka.getStorageByPoolName();
    }

    /**
     * This is used to get the resource configuration used by the Kafka brokers. The resource configuration is needed by
     * Cruise Control. But collecting it directly in Cruise Control from the custom resources would be complicated as
     * it would need to figure out the node pools and the node IDs belonging to them. This includes only the broker
     * nodes. Controller nodes are not included in this map.
     *
     * @return  Map with the pool names as the keys and resource requirements for given pools as the values
     */
    public Map<String, ResourceRequirements> kafkaBrokerResourceRequirements()   {
        return kafka.getBrokerResourceRequirementsByPoolName();
    }

    /**
     * This returns the list of Kafka brokers which will be later used for Cruise Control configuration so that Cruise
     * Control does not need to collect it itself from the different custom resources. This includes only the broker
     * nodes. Controller nodes are not included in this set.
     *
     * @return  Set with node references for the Kafka nodes
     */
    public Set<NodeRef> kafkaBrokerNodes()   {
        return kafka.brokerNodes();
    }
}
