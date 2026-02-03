/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.TimeoutException;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class KafkaRoller2Test {

    // Test constants
    static final Function<Integer, String> EMPTY_CONFIG_SUPPLIER = serverId -> "";
    private final Map<Integer, Long> defaultQuorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
    private final List<Integer> restartedNodesOrder = new java.util.ArrayList<>();
    private final List<List<Integer>> restartBatches = new ArrayList<>();
    private List<Integer> currentBatch = new ArrayList<>();

    @BeforeEach
    void setUp() {
        // Clear restart tracking between tests
        restartedNodesOrder.clear();
        restartBatches.clear();
        currentBatch = new ArrayList<>();
    }

    static RestartReasons noReasons(Pod pod) {
        return RestartReasons.empty();
    }

    private static RestartReasons manualRolling(Pod pod) {
        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
    }

    private static RestartReasons podHasOldRevision(Pod pod) {
        return RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION);
    }

    private PlatformClient mockedPlatformClient() {
        PlatformClient platformClient = mock(PlatformClient.class);
        doReturn(PlatformClient.PodState.READY)
                .when(platformClient)
                .podState(any());

        doReturn(CompletableFuture.completedFuture(null))
                .when(platformClient)
                .podReadiness(any());

        // Track restart order
        doAnswer(invocation -> {
            NodeRef nodeRef = invocation.getArgument(0);
            int nodeId = nodeRef.nodeId();
            restartedNodesOrder.add(nodeId);
            currentBatch.add(nodeId);
            return CompletableFuture.completedFuture(null);
        }).when(platformClient).restartPod(any(), any());

        // When we check pod state AFTER a restart, we know the batch is complete
        doAnswer(invocation -> {
            if (!currentBatch.isEmpty()) {
                restartBatches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
            }
            return CompletableFuture.completedFuture(null);
        }).when(platformClient).podReadiness(any());

        return platformClient;
    }

    private KafkaRollerClient mockedKafkaRollerClient() throws InterruptedException {
        KafkaRollerClient kafkaRollerClient = mock(KafkaRollerClient.class);

        doReturn(0)
                .when(kafkaRollerClient)
                .tryElectAllPreferredLeaders(any());

        doReturn(true).when(kafkaRollerClient).canConnectToNode(any(), anyBoolean());

        return kafkaRollerClient;
    }

    private KafkaAgentClient mockedKafkaAgentClient() {
        KafkaAgentClient kafkaAgentClient = mock(KafkaAgentClient.class);
        BrokerState brokerstate = new BrokerState(3, null);

        doReturn(brokerstate)
                .when(kafkaAgentClient)
                .getBrokerState(any());

        return kafkaAgentClient;
    }

    /**
     * Builder for creating test topologies with mocked Kafka clusters.
     * Manages the setup of nodes, topics, and their associated mocks.
     */
    static class MockBuilder {
        private final Map<Integer, NodeRef> nodeRefs = new LinkedHashMap<>();
        private final Map<Integer, Node> nodes = new LinkedHashMap<>();
        private final Set<TopicListing> topicListing = new HashSet<>();
        private final Map<String, Integer> topicMinIsrs = new HashMap<>();
        private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();


        /**
         * Adds a Kafka node to the test topology.
         * @param platformClient The platform client mock to configure
         * @param controller Whether this node is a controller
         * @param broker Whether this node is a broker
         * @param nodeId The node ID
         * @return this builder for chaining
         */
        MockBuilder addNode(PlatformClient platformClient, boolean controller, boolean broker, int nodeId) {
            if (nodeRefs.containsKey(nodeId)) {
                throw new IllegalStateException("Node " + nodeId + " already exists");
            }
            if (!controller && !broker) {
                throw new IllegalArgumentException("Node must be either a controller or a broker (or both)");
            }

            nodeRefs.put(nodeId, new NodeRef("pool-kafka-" + nodeId, nodeId, "pool", controller, broker));
            nodes.put(nodeId, new Node(nodeId, "pool-kafka-" + nodeId, 9092));
            doReturn(new KafkaNodeRoles(controller, broker)).when(platformClient).kafkaNodeRoles(nodeRefs.get(nodeId));
            return this;
        }

        /**
         * Adds multiple nodes with the same role configuration.
         * @param platformClient The platform client mock to configure
         * @param controller Whether these nodes are controllers
         * @param broker Whether these nodes are brokers
         * @param nodeIds The node IDs to add
         * @return this builder for chaining
         */
        MockBuilder addNodes(PlatformClient platformClient, boolean controller, boolean broker, int... nodeIds) {
            for (int nodeId : nodeIds) {
                addNode(platformClient, controller, broker, nodeId);
            }
            return this;
        }

        MockBuilder mockNodeState(PlatformClient platformClient, List<PlatformClient.PodState> states, int nodeId) {
            doReturn(states.getFirst(), states.size() == 1 ? new Object[0] : states.subList(1, states.size()).toArray())
                    .when(platformClient)
                    .podState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockNotReadyPod(PlatformClient platformClient, int nodeId) {
            doReturn(CompletableFuture.failedFuture(new TimeoutException()))
                    .when(platformClient)
                    .podReadiness(nodeRefs.get(nodeId));
            return this;
        }

        //This emulates pod that is initially not ready but ready after the restart
        MockBuilder mockNotReadyAndThenReadyPod(PlatformClient platformClient, int nodeId) {
            doReturn(CompletableFuture.failedFuture(new TimeoutException()), CompletableFuture.completedFuture(null))
                    .when(platformClient)
                    .podReadiness(nodeRefs.get(nodeId));
            return this;
        }


        MockBuilder mockNodeBecomesReady(PlatformClient platformClient, int nodeId, PlatformClient.PodState state, int retriesUntilReady) {
            List<PlatformClient.PodState> states = new ArrayList<>();
            for (int i = 0; i < retriesUntilReady; i++) {
                states.add(state);
            }
            states.add(PlatformClient.PodState.READY);
            return mockNodeState(platformClient, states, nodeId);
        }

        MockBuilder mockBrokerStateNotReady(KafkaAgentClient kafkaAgentClient, int nodeId) {
            doReturn(new BrokerState(1, null))
                    .when(kafkaAgentClient)
                    .getBrokerState(nodeRefs.get(nodeId).podName());
            return this;
        }

        MockBuilder mockBrokerStateRecovery(KafkaAgentClient kafkaAgentClient, int nodeId) {
            Map<String, Object> recoveryState = new HashMap<>();
            recoveryState.put("remainingLogsToRecover", 100);
            recoveryState.put("remainingSegmentsToRecover", 300);

            doReturn(new BrokerState(2, recoveryState))
                    .when(kafkaAgentClient)
                    .getBrokerState(nodeRefs.get(nodeId).podName());
            return this;
        }

        MockBuilder addTopic(String topicName, int leaderId, List<Integer> replicaIds, List<Integer> isrIds) {
            return addTopic(topicName, leaderId, replicaIds, isrIds, null);
        }

        MockBuilder addTopic(String topicName, int leaderId, List<Integer> replicaIds, List<Integer> isrIds, Integer minIsr) {
            if (!replicaIds.contains(leaderId)) {
                throw new RuntimeException("Leader is not a replica");
            }
            for (var isrId : isrIds) {
                if (!replicaIds.contains(isrId)) {
                    throw new RuntimeException("ISR is not a subset of replicas");
                }
            }
            if (topicListing.stream().anyMatch(tl -> tl.name().equals(topicName))) {
                throw new RuntimeException("Topic " + topicName + " already exists");
            }
            Uuid topicId = Uuid.randomUuid();
            topicListing.add(new TopicListing(topicName, topicId, false));

            Node leaderNode = nodes.get(leaderId);
            List<Node> replicas = replicaIds.stream().map(nodes::get).toList();
            List<Node> isr = isrIds.stream().map(nodes::get).toList();
            topicDescriptions.put(topicId, new TopicDescription(topicName, false,
                    List.of(new TopicPartitionInfo(0,
                            leaderNode, replicas, isr))));

            topicMinIsrs.put(topicName, minIsr);
            return this;
        }

        MockBuilder mockTopics(KafkaRollerClient client) throws InterruptedException {
            doReturn(topicListing)
                    .when(client)
                    .listTopics();

            doAnswer(i -> {
                List<Uuid> topicIds = i.getArgument(0);
                return topicIds.stream().map(topicDescriptions::get).toList();
            })
                    .when(client)
                    .describeTopics(any());
            doAnswer(i -> {
                List<String> topicNames = i.getArgument(0);
                Map<String, Config> map = new HashMap<>();
                for (String topicName : topicNames) {
                    Integer minIsr = topicMinIsrs.get(topicName);
                    Collection<ConfigEntry> configEntries = new ArrayList<>();
                    if (minIsr != null) {
                        configEntries.add(new ConfigEntry("min.insync.replicas", minIsr.toString()));
                    }
                    if (map.put(topicName, new Config(configEntries)) != null) {
                        throw new IllegalStateException("Duplicate key");
                    }
                }
                return map;
            })
                    .when(client)
                    .describeTopicConfigs(any());
            return this;
        }

        MockBuilder mockDescribeConfigs(KafkaRollerClient kafkaRollerClient, Set<ConfigEntry> configEntries, int... nodeIds) throws InterruptedException {
            Config config = new Config(configEntries);
            for (var nodeId : nodeIds) {
                doReturn(config)
                        .when(kafkaRollerClient)
                        .describeBrokerConfigs(nodeId);
                doReturn(config)
                        .when(kafkaRollerClient)
                        .describeControllerConfigs(nodeId);
            }
            return this;
        }

        MockBuilder mockQuorumCheck(KafkaRollerClient kafkaRollerClient, int leaderId, Map<Integer, Long> quorumState) throws InterruptedException {
            QuorumInfo quorumInfo = mock(QuorumInfo.class);
            when(quorumInfo.leaderId()).thenReturn(leaderId);

            List<QuorumInfo.ReplicaState> replicaStates = new ArrayList<>();
            quorumState.forEach((key, value) -> {
                QuorumInfo.ReplicaState replicaState = mock(QuorumInfo.ReplicaState.class);

                when(replicaState.replicaId()).thenReturn(key);
                when(replicaState.lastCaughtUpTimestamp()).thenReturn(
                       value >= 0
                                ? java.util.OptionalLong.of(value)
                                : java.util.OptionalLong.empty()
                );
                replicaStates.add(replicaState);
            });

            when(quorumInfo.voters()).thenReturn(replicaStates);
            when(kafkaRollerClient.describeMetadataQuorum()).thenReturn(quorumInfo);
            when(kafkaRollerClient.getActiveControllerId()).thenReturn(leaderId);
            return this;
        }

        MockBuilder mockNodeBecomesConnectable(KafkaRollerClient rollerClient, int nodeId, int retriesUntilConnectable) {
            // In each retry, we query the state twice (one during the initialise step and another one during the wait for unready)
            List<Boolean> connectionStates = new ArrayList<>();
            for (int i = 0; i < retriesUntilConnectable; i++) {
                connectionStates.add(false);
            }
            connectionStates.add(true);
            return mockConnectionToNode(rollerClient, connectionStates, nodeId);
        }

        MockBuilder mockConnectionToNode(KafkaRollerClient kafkaRollerClient, List<Boolean> connectionStates, int nodeId) {
            doReturn(connectionStates.getFirst(), connectionStates.size() == 1 ? new Object[0] : connectionStates.subList(1, connectionStates.size()).toArray())
                    .when(kafkaRollerClient)
                    .canConnectToNode(eq(nodeRefs.get(nodeId)), anyBoolean());
            return this;
        }

        Map<Integer, NodeRef> done() {
            return nodeRefs;
        }
    }


    /**
     * Helper method to verify that specific nodes were restarted in the expected order.
     * @param expectedNodeIds The expected node IDs in the order they should have been restarted
     */
    private void assertRestartedNodesOrder(int... expectedNodeIds) {
        List<Integer> expected = IntStream.of(expectedNodeIds).boxed().toList();
        assertThat("Restarted nodes order mismatch", restartedNodesOrder, is(expected));
    }

    private CompletableFuture<Void> doRollingRestart(PlatformClient platformClient,
                                                     KafkaRollerClient kafkaRollerClient,
                                                     KafkaAgentClient kafkaAgentClient,
                                                     Collection<NodeRef> nodeRefList,
                                                     Function<Pod, RestartReasons> reason,
                                                     Function<Integer, String> kafkaConfigProvider,
                                                     int maxRestartsBatchSize) {

        var kafkaRoller = KafkaRoller2.initialise(
                platformClient,
                kafkaRollerClient,
                kafkaAgentClient,
                nodeRefList,
                mockPodOperator(),
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                true,
                kafkaConfigProvider,
                1_000,
                500,
                maxRestartsBatchSize,
                () -> new BackOff(10L, 2, 3));

        CompletableFuture<Void> result = new CompletableFuture<>();
        kafkaRoller.rollingRestart().onComplete(ar -> {
            if (ar.succeeded()) {
                result.complete(ar.result());
            } else {
                result.completeExceptionally(ar.cause());
            }
        });

        return result;
    }

    private PodOperator mockPodOperator() {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(any(), any())).thenAnswer(
                invocation -> new PodBuilder()
                        .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName("pool-kafka-" + invocation.getArgument(1))
                        .endMetadata()
                        .build()
        );
        return podOps;
    }

    //////////////////////////////////////////////////////
    /// Test scenarios we expect restarts              ///
    //////////////////////////////////////////////////////

    @Test
    public void shouldRestartManualRollingUpdate() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        Map<Integer, Long> quorumStates = new HashMap<>();
        quorumStates.putAll(defaultQuorumState);
        quorumStates.putAll(Map.of(3, 10_000L, 4, 10_000L, 5, 10_000L));

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .addNodes(platformClient, true, true, 3, 4, 5)
                .addNodes(platformClient, false, true, 6, 7, 8)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumStates)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    // The order we expect is controllers (0, 2), combined nodes (3, 4, 5), active controller (1), then brokers (6, 7, 8)
                    assertRestartedNodesOrder(0, 2, 3, 4, 5, 1, 6, 7, 8);

                    // Verify brokers had leader election
                    for (int nodeId : List.of(3, 4, 5, 6, 7, 8)) {
                        try {
                            Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }

    @Test
    public void shouldRestartUnreadyControllerFirst() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient agentClient = mockedKafkaAgentClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .mockNodeBecomesReady(platformClient, 2, PlatformClient.PodState.NOT_READY, 1)
                .mockNotReadyAndThenReadyPod(platformClient, 2)
                .mockBrokerStateNotReady(agentClient, 2) // broker state < 3 for unready
                .mockQuorumCheck(kafkaRollerClient, 0, defaultQuorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, agentClient, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    // The order we expect is unready controller (in this case combined), ready controller, active controller
                    assertRestartedNodesOrder(2, 1, 0);
                }).get();
    }

    @Test
    public void shouldRestartUnreadyBrokerFirst() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient agentClient = mockedKafkaAgentClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .mockNodeBecomesReady(platformClient, 2, PlatformClient.PodState.NOT_READY, 1)
                .mockNotReadyAndThenReadyPod(platformClient, 2)
                .mockBrokerStateNotReady(agentClient, 2) // broker state < 3 for unready
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, agentClient, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    // The order we expect is unready broker, ready broker
                    assertRestartedNodesOrder(2, 0, 1);
                }).get();
    }

    @Test
    public void shouldRestartNotRunningNodes() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockNodeBecomesReady(platformClient, 0, PlatformClient.PodState.NOT_RUNNING, 1)
                .mockNodeBecomesReady(platformClient, 2, PlatformClient.PodState.NOT_RUNNING, 1)
                .mockNodeBecomesReady(platformClient, 4, PlatformClient.PodState.NOT_RUNNING, 2) // In each retry, we only restart 1 set of nodes or 1 node at a time. We expect node 4 to be restarted on the second retry
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::podHasOldRevision, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    // The order we expect parallel restart of controllers and then broker, then healthy nodes
                    assertRestartBatches(List.of(
                            List.of(0, 2),
                            List.of(4),
                            List.of(1),
                            List.of(3)
                    ));

                    // Verify brokers had leader election
                    for (int nodeId : List.of(2, 3, 4)) {
                        try {
                            Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }

    @Test
    public void shouldRestartUnresponsiveNodes() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockNodeBecomesConnectable(kafkaRollerClient, 0, 1)
                .mockNodeBecomesConnectable(kafkaRollerClient, 4, 2) // We restart only a single node in each retry, so node 4 would be restarted in the second round
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::noReasons, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    // The order we expect parallel unresponsive controller, combined node and broker
                    assertRestartedNodesOrder(0, 4);

                    // Verify broker had leader election
                    try {
                        Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(4)));
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }

    @Test
    public void shouldRestartNonDynamicConfig() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .mockDescribeConfigs(kafkaRollerClient, Set.of(new ConfigEntry("auto.create.topics.enable", "true")), 0, 1, 2, 3, 4)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::noReasons, nodeId -> "auto.create.topics.enable=false", 1)
                .whenComplete((r, e) -> {
                    assertRestartedNodesOrder(0, 2, 1, 3, 4);

                    // Verify brokers had leader election
                    for (int nodeId : List.of(2, 3, 4)) {
                        try {
                            Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                    for (var nodeRef : nodeRefs.values()) {
                        try {
                            Mockito.verify(kafkaRollerClient, never()).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }).get();
    }

    @Test
    public void shouldRestartWhenDescribeConfigFailed() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        doThrow(new KafkaException("Error getting Kafka config")).when(kafkaRollerClient).describeBrokerConfigs(anyInt());
        doThrow(new KafkaException("Error getting Kafka config")).when(kafkaRollerClient).describeControllerConfigs(anyInt());

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        // When describeConfigs fails, CONFIG_CHANGE_REQUIRES_RESTART is added and nodes should be restarted
        doRollingRestart(platformClient,
                kafkaRollerClient,
                null,
                nodeRefs.values(),
                KafkaRoller2Test::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                1
        ).whenComplete((r, e) -> {
            assertRestartedNodesOrder(0, 2, 1, 3, 4);
            try {
                Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }).get();
    }

    @Test
    public void shouldRestartBrokerDynamicConfigFailed() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        doThrow(new RuntimeException("Configuration update failed")).when(kafkaRollerClient).reconfigureNode(any(), any(), anyBoolean());

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .mockDescribeConfigs(kafkaRollerClient, Set.of(new ConfigEntry("min.insync.replicas", "1")), 0, 1, 2, 3, 4)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::noReasons, nodeId -> "min.insync.replicas=2", 1)
                .whenComplete((r, e) -> {
                    assertRestartedNodesOrder(0, 2, 1, 3, 4);

                    for (var nodeRef : nodeRefs.values()) {
                        try {
                            Mockito.verify(kafkaRollerClient, times(1)).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        if (nodeRef.broker()) {
                            try {
                                Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
                            } catch (InterruptedException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                }).get();
    }


    @Test
    public void shouldRestartTwoNodesQuorumControllers() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mock(KafkaAgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, kafkaAgentClient, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
                .whenComplete((r, e) -> {
                    assertRestartedNodesOrder(2, 1);

                    // Verify controllers did not have leader election
                    for (int nodeId : List.of(1, 2)) {
                        try {
                            Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }

    @Test
    public void shouldRestartTwoNodesQuorumOneControllerBehind() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mock(KafkaAgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 7_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        var ex = assertThrows(ExecutionException.class, () ->
                doRollingRestart(platformClient, kafkaRollerClient, kafkaAgentClient, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3).get());

        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));

        // Only the controller that has fallen behind should be restarted
        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(1)), any());
    }

    @Test
    public void shouldRestartSingleNodeQuorum() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mock(KafkaAgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, kafkaAgentClient, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
                .whenComplete((r, e) -> {
                    assertRestartedNodesOrder(1, 2);

                    // Verify broker had leader election
                    try {
                        Mockito.verify(kafkaRollerClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(2)));
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                    // Verify controller did not have leader election
                    try {
                        Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }


    //////////////////////////////////////////////////////
    /// Test scenarios we expect no restarts           ///
    //////////////////////////////////////////////////////

    @Test
    void shouldNotRestartNoReason() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::noReasons, EMPTY_CONFIG_SUPPLIER, 1)
                .whenComplete((r, e) -> {
                    try {
                        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    Mockito.verify(platformClient, never()).restartPod(any(), any());
                    try {
                        Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(any());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get();
    }

    @Test
    void shouldThrowFatalExceptionWhenNotReadyAfterRestart() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mockedKafkaAgentClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockNotReadyPod(platformClient, 0)
                .mockBrokerStateNotReady(kafkaAgentClient, 0) // When PodState is NOT_READY, BrokerState is checked. It is set to < 3 for not ready brokers
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        kafkaAgentClient,
                        nodeRefs.values(),
                        KafkaRoller2Test::podHasOldRevision,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertThat(ex.getCause() instanceof KafkaRoller2.FatalException, is(true));
        assertThat(ex.getCause().getMessage().contains("Error while waiting for restarted pod pool-kafka-0/0 to become ready"), is(true));

        assertRestartedNodesOrder(0);
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(3)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(4)), any());
        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
        Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldThrowFatalExceptionWhenNotReadyNoReason() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mockedKafkaAgentClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.PodState.NOT_READY), 0)
                .mockNotReadyPod(platformClient, 0)
                .mockBrokerStateNotReady(kafkaAgentClient, 0) // setting to < 3 for not ready brokers
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::noReasons,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertThat(ex.getCause() instanceof KafkaRoller2.FatalException, is(true));
        assertThat(ex.getCause().getMessage().contains("Error while waiting for non restarted pod"), is(true));

        Mockito.verify(platformClient, never()).restartPod(any(), any());
        Mockito.verify(kafkaRollerClient, never()).reconfigureNode(any(), any(), anyBoolean());
        Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldNotRestartNotRunningWhenDoesNotHaveOldRevision() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.PodState.NOT_RUNNING), 0)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::noReasons,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertThat(ex.getCause() instanceof KafkaRoller2.FatalException, is(true));
        assertEquals("Pod pool-kafka-0 is unschedulable or is not starting", ex.getCause().getMessage());

        Mockito.verify(platformClient, never()).restartPod(any(), any());
        Mockito.verify(kafkaRollerClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldNotRestartWhenQuorumCheckFailed() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 6000L);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNode(platformClient, true, true, 2)
                .mockDescribeConfigs(kafkaRollerClient, Set.of(new ConfigEntry("controller.quorum.fetch.timeout.ms", "3000")), 0, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The active controller and the up-to-date follower should not be restarted but fallen-behind follower can be restarted as doesn't impact the quorum health
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartWhenAvailabilityCheckFailed() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1), 2)
                .mockTopics(kafkaRollerClient)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The partition leader and in sync replica should not be restarted but out of sync replica can be restarted as doesn't impact the availability
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartCombinedNodesWhenAvailabilityCheckFailed() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1), 2)
                .mockTopics(kafkaRollerClient)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The partition leader and in sync replica should not be restarted but out of sync replica can be restarted as doesn't impact the availability
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));

        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartBrokerNodeInRecovery() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mockedKafkaAgentClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.PodState.NOT_READY), 2)
                .mockNotReadyPod(platformClient, 2)
                .mockBrokerStateRecovery(kafkaAgentClient, 2)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        kafkaAgentClient,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        Mockito.verify(platformClient, never()).restartPod(any(), any());
    }

    @Test
    void shouldNotRestartControllerNodeInRecovery() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mockedKafkaAgentClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.PodState.NOT_READY), 2)
                .mockBrokerStateRecovery(kafkaAgentClient, 2)
                .done();


        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        kafkaAgentClient,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        Mockito.verify(platformClient, never()).restartPod(any(), any());
    }

    @Test
    public void shouldNotRestartEvenSizedQuorumTwoControllersBehind() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L, 3, 6000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2, 4) //combined nodes
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        Mockito.verify(platformClient, never()).restartPod(any(), any());
    }

    @Test
    public void shouldNotRestartControllersWithInvalidTimestamp() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        Map<Integer, Long> quorumState = Map.of(0, -1L, 1, 10_000L, 2, -1L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, 1, quorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        Mockito.verify(platformClient, never()).restartPod(any(), any());
    }

    @Test
    public void shouldNotRollControllersWithInvalidLeader() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockQuorumCheck(kafkaRollerClient, -1, defaultQuorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        null,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
        Mockito.verify(platformClient, never()).restartPod(any(), any());
    }

    @Test
    public void shouldThrowExceptionInitAdminException() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        doThrow(new RuntimeException("Failed to create admin client for brokers")).when(kafkaRollerClient).initialiseBrokerAdmin(any());

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .done();

        var ex = assertThrows(ExecutionException.class, () ->
                doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3).get());

        // Should fail with TimeoutException after retries are exhausted
        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));
    }

    @Test
    public void shouldNotRestartDynamicConfig() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3, 4)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .mockDescribeConfigs(kafkaRollerClient, Set.of(new ConfigEntry("min.insync.replicas", "1")), 0, 1, 2, 3, 4)
                .done();

        doRollingRestart(platformClient,
                kafkaRollerClient,
                null,
                nodeRefs.values(),
                KafkaRoller2Test::noReasons,
                nodeId -> "min.insync.replicas=2",
                1
        ).whenComplete((r, e) -> {
            for (var nodeRef : nodeRefs.values()) {
                try {
                    Mockito.verify(kafkaRollerClient, times(1)).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                Mockito.verify(platformClient, never()).restartPod(eq(nodeRef), any());
            }
        }).get();
    }


    //////////////////////////////////////////////////////
    /// Test scenarios for batch rolling               ///
    //////////////////////////////////////////////////////

    private void assertRestartBatches(List<List<Integer>> expectedBatches) {
        assertEquals(expectedBatches, restartBatches);
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatched() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockTopics(kafkaRollerClient)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
                .whenComplete((r, e) -> assertRestartBatches(List.of(
                        List.of(0), // Batch 1: controller 0
                        List.of(2), // Batch 2: controller 2
                        List.of(1), // Batch 3: active controller 1
                        List.of(5, 8),  // Batch 4: brokers 5 and 8 in parallel
                        List.of(3, 6),  // Batch 5: brokers 3 and 6 in parallel
                        List.of(4, 7)// Batch 6: brokers 4 and 7 in parallel
                ))).get();
    }

    @Test
    public void shouldRestartCombinedNodesInExpectedOrderAndBatched() throws ExecutionException, InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        Map<Integer, Long> quorumState = Map.of(3, 10_000L,
                4, 10_000L,
                5, 10_000L,
                6, 10_000L,
                7, 10_000L,
                8, 5_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, // combined nodes
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockTopics(kafkaRollerClient)
                .mockQuorumCheck(kafkaRollerClient, 3, quorumState)
                .done();

        doRollingRestart(platformClient, kafkaRollerClient, null, nodeRefs.values(), KafkaRoller2Test::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
                .whenComplete((r, e) -> {
                    // The expected order to restart nodes individually based on the availability and quorum health and then the broker that is the active controller will be started at last
                    assertRestartBatches(List.of(
                            List.of(6),
                            List.of(4),
                            List.of(7),
                            List.of(5),
                            List.of(8),
                            List.of(3) // active controller
                    ));
                }).get();
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatchedWithUrp() throws InterruptedException {
        PlatformClient platformClient = mockedPlatformClient();
        KafkaRollerClient kafkaRollerClient = mockedKafkaRollerClient();
        KafkaAgentClient kafkaAgentClient = mock(KafkaAgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                // topic A is at its min ISR, so neither 3 nor 4 should be restarted
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4), 2)
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockTopics(kafkaRollerClient)
                .mockQuorumCheck(kafkaRollerClient, 1, defaultQuorumState)
                .done();

        var ex = assertThrows(ExecutionException.class,
                () -> doRollingRestart(platformClient,
                        kafkaRollerClient,
                        kafkaAgentClient,
                        nodeRefs.values(),
                        KafkaRoller2Test::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        3
                ).get());

        assertThat(ex.getCause() instanceof io.strimzi.operator.common.TimeoutException, is(true));

        // The expected order is non-active controller nodes, the active controller,
        // batches of brokers starting with the largest.
        // Verify nodes 3 and 4 were not restarted due to availability constraints
        assertRestartBatches(List.of(
                List.of(0), // Batch 1: controller 0
                List.of(2), // Batch 2: controller 2
                List.of(1), // Batch 3: active controller 1
                List.of(5, 8),  // Batch 4: brokers 5 and 8 in parallel
                List.of(6), // Batch 5: brokers 6 and 3 can be restarted in parallel but 3 is not safe to restart
                List.of(7)  // Batch 6: brokers 4 and 7 can be restarted in parallel but 4 is not safe to restart

        ));

        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(3)), any());
        Mockito.verify(platformClient, never()).restartPod(eq(nodeRefs.get(4)), any());

    }
}