/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.operator.cluster.operator.resource.rolling.RackRolling.CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;
import static io.strimzi.operator.cluster.operator.resource.rolling.RackRolling.CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class RackRollingTest {

    static final Function<Integer, String> EMPTY_CONFIG_SUPPLIER = serverId -> "";

    private final Time time = new Time.TestTime(1_000_000_000L);
    private static Vertx vertx;

    private final Map<Integer, Long> defaultQuorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);

    // List to track the order of restarted nodes
    private final List<Integer> restartedNodesOrder = new java.util.ArrayList<>();

    static RestartReasons noReasons(Pod pod) {
        return RestartReasons.empty();
    }

    private static RestartReasons manualRolling(Pod pod) {
        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
    }

    private static RestartReasons podUnresponsive(Pod pod) {
        return RestartReasons.of(RestartReason.POD_UNRESPONSIVE);
    }

    private static RestartReasons podHasOldRevision(Pod pod) {
        return RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION);
    }

    private PlatformClient mockedPlatformClient() {
        PlatformClient platformClient = mock(PlatformClient.class);
        doReturn(PlatformClient.NodeState.READY)
                .when(platformClient)
                .nodeState(any());

        // Track restart order
        doAnswer(invocation -> {
            NodeRef nodeRef = invocation.getArgument(0);
            restartedNodesOrder.add(nodeRef.nodeId());
            return null;
        }).when(platformClient).restartNode(any(), any());

        return platformClient;
    }

    private RollClient mockedRollClient() {
        RollClient rollClient = mock(RollClient.class);

        doReturn(Collections.emptyMap())
                .when(rollClient)
                .describeBrokerConfigs(any());
        doReturn(Collections.emptyMap())
                .when(rollClient)
                .describeControllerConfigs(any());

        doReturn(0)
                .when(rollClient)
                .tryElectAllPreferredLeaders(any());

        doReturn(true).when(rollClient).canConnectToNode(any(), anyBoolean());

        return rollClient;
    }

    static class MockBuilder {
        private final Map<Integer, NodeRef> nodeRefs = new LinkedHashMap<>();
        private final Map<Integer, Node> nodes = new LinkedHashMap<>();
        private final Set<TopicListing> topicListing = new HashSet<>();
        private final Map<String, Integer> topicMinIsrs = new HashMap<>();
        private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();
        private final Map<Integer, Config> nodeConfigs = new HashMap<>();


        MockBuilder addNode(PlatformClient platformClient, boolean controller, boolean broker, int nodeId) {
            if (nodeRefs.containsKey(nodeId)) {
                throw new RuntimeException();
            }
            if (!controller && !broker) {
                throw new RuntimeException();
            }

            nodeRefs.put(nodeId, new NodeRef("pool-kafka-" + nodeId, nodeId, "pool", controller, broker));
            nodes.put(nodeId, new Node(nodeId, "pool-kafka-" + nodeId, 9092));
            doReturn(new NodeRoles(controller, broker)).when(platformClient).nodeRoles(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder addNodes(PlatformClient platformClient, boolean controller, boolean broker, int... nodeIds) {
            for (int nodeId : nodeIds) {
                addNode(platformClient, controller, broker, nodeId);
            }
            return this;
        }

        MockBuilder mockNodeState(PlatformClient platformClient, List<PlatformClient.NodeState> nodeStates, int nodeId) {
            doReturn(nodeStates.get(0), nodeStates.size() == 1 ? new Object[0] : nodeStates.subList(1, nodeStates.size()).toArray())
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockHealthyNodes(PlatformClient platformClient, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockHealthyNode(platformClient, nodeId);
            }
            return this;
        }

        private void mockHealthyNode(PlatformClient platformClient, int nodeId) {
            doReturn(PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
        }

        MockBuilder mockNotRunningNodes(PlatformClient platformClient, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockNotRunningNode(platformClient, nodeId);
            }
            return this;
        }

        private void mockNotRunningNode(PlatformClient platformClient, int nodeId) {
            doReturn(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
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

        MockBuilder mockTopics(RollClient client) {
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
                Map<String, Integer> map = new HashMap<>();
                for (String topicName : topicNames) {
                    if (map.put(topicName, topicMinIsrs.get(topicName)) != null) {
                        throw new IllegalStateException("Duplicate key");
                    }
                }
                return map;
            })
                    .when(client)
                    .describeTopicMinIsrs(any());
            return this;
        }

        MockBuilder mockDescribeConfigs(RollClient rollClient, Set<ConfigEntry> configEntries, int... nodeIds) {
            Set<ConfigEntry> configs = new HashSet<>();
            // This config should always be returned for controllers
            if (configEntries.isEmpty() || configEntries.stream().noneMatch(configEntry -> configEntry.name().contains(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME))) {
                configs.add(new ConfigEntry(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME, String.valueOf(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT),
                        ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false, Collections.emptyList(), null, null));
            }
            configs.addAll(configEntries);

            for (var nodeId : nodeIds) {
                if (!this.nodeConfigs.containsKey(nodeId)) this.nodeConfigs.put(nodeId, new Config(configs));
            }
            doReturn(this.nodeConfigs)
                    .when(rollClient)
                    .describeBrokerConfigs(any());
            doReturn(this.nodeConfigs)
                    .when(rollClient)
                    .describeControllerConfigs(any());

            return this;
        }

        MockBuilder mockReconfigureConfigs(RollClient rollClient) {
            doAnswer(i -> {
                NodeRef nodeRef = i.getArgument(0);
                KafkaConfigurationDiff diff = i.getArgument(1);
                var nodeConfigs = diff.getConfigDiff().stream().map(AlterConfigOp::configEntry).collect(Collectors.toSet());
                this.nodeConfigs.put(nodeRef.nodeId(), new Config(nodeConfigs));
                return null;
            })
                    .when(rollClient)
                    .reconfigureNode(any(), any(), anyBoolean());
            return this;
        }

        MockBuilder mockQuorumLastCaughtUpTimestamps(RollClient rollClient, Map<Integer, Long> quorumState) {

            Admin admin = mock(Admin.class);
            DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
            when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
            KafkaFuture<QuorumInfo> kafkaFuture = KafkaFuture.completedFuture(null);
            when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);

            doReturn(quorumState)
                    .when(rollClient)
                    .quorumLastCaughtUpTimestamps(any());

            doReturn(quorumState)
                    .when(rollClient)
                    .quorumLastCaughtUpTimestamps(any());
            return this;
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int... nodeIds) {
            return mockElectLeaders(rollClient, List.of(0), nodeIds);
        }

        MockBuilder mockSuccessfulConnection(RollClient rollClient, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockConnectionToNode(rollClient, nodeId);
            }
            return this;
        }

        private void mockConnectionToNode(RollClient rollClient, int nodeId) {
            doReturn(true).when(rollClient).canConnectToNode(eq(nodeRefs.get(nodeId)), anyBoolean());
        }

        MockBuilder mockConnectionToNode(RollClient rollClient, List<Boolean> connectionStates, int nodeId) {
            doReturn(connectionStates.get(0), connectionStates.size() == 1 ? new Object[0] : connectionStates.subList(1, connectionStates.size()).toArray())
                    .when(rollClient)
                    .canConnectToNode(eq(nodeRefs.get(nodeId)), anyBoolean());
            return this;
        }

        MockBuilder mockElectLeaders(RollClient rollClient, List<Integer> results, int... nodeIds) {
            for (var nodeId : nodeIds) {
                doReturn(results.get(0), results.subList(1, results.size()).toArray())
                        .when(rollClient)
                        .tryElectAllPreferredLeaders(nodeRefs.get(nodeId));
            }
            return this;
        }

        Map<Integer, NodeRef> done() {
            return nodeRefs;
        }

        MockBuilder mockLeader(RollClient rollClient, int leaderId) {
            doReturn(leaderId).when(rollClient).activeController();
            return this;
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

    /**
     * Helper method to clear the restart order tracking between test iterations.
     */
    private void clearRestartOrder() {
        restartedNodesOrder.clear();
    }

    private RackRolling newRollingRestart(PlatformClient platformClient,
                                RollClient rollClient,
                                AgentClient agentClient,
                                Collection<NodeRef> nodeRefList,
                                Function<Pod, RestartReasons> reason,
                                Function<Integer, String> kafkaConfigProvider,
                                boolean allowReconfiguration,
                                int maxRestartsBatchSize) {
        return RackRolling.initialise(time,
                platformClient,
                rollClient,
                agentClient,
                nodeRefList,
                mockPodOperator(),
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                allowReconfiguration,
                kafkaConfigProvider,
                1_000,
                500,
                maxRestartsBatchSize,
                () -> new BackOff(10L, 2, 3));
    }


    private CompletableFuture<Void> doRollingRestart(PlatformClient platformClient,
                                  RollClient rollClient,
                                  AgentClient agentClient,
                                  Collection<NodeRef> nodeRefList,
                                  Function<Pod, RestartReasons> reason,
                                  Function<Integer, String> kafkaConfigProvider,
                                  int maxRestartsBatchSize) {

        var rr = RackRolling.initialise(time,
                platformClient,
                rollClient,
                agentClient,
                nodeRefList,
                mockPodOperator(),
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                true,
                kafkaConfigProvider,
                120_000,
                500,
                maxRestartsBatchSize,
                () -> new BackOff(10L, 2, 3));

        CompletableFuture<Void> result = new CompletableFuture<>();
        rr.rollingRestart().onComplete(ar -> {
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

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    //////////////////////////////////////////////////////
    /// Test scenarios we expect restarts              ///
    //////////////////////////////////////////////////////

    @Test
    public void shouldRestartManualRollingUpdate() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        Map<Integer, Long> quorumState = Map.of(3, 10_000L, 4, 10_000L, 5, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .addNodes(platformClient, true, false, 3, 4, 5)
                .mockLeader(rollClient, 3)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4, 5)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1, 2))
                .addTopic("topic-B", 1, List.of(1, 2, 0), List.of(1, 2, 0))
                .addTopic("topic-C", 2, List.of(2, 0, 1), List.of(2, 0, 1))
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                null,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                1);

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                // The order we expect is controllers (4, 5), active controller (3), then brokers (0, 1, 2)
                assertRestartedNodesOrder(4, 5, 3, 0, 1, 2);

                // Verify brokers had leader election
                for (int nodeId : List.of(0, 1, 2)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify controllers did not have leader election
                for (int nodeId : List.of(3, 4, 5)) {
                    Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4, 5)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartCombinedNodesManualRollingUpdate() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2)
                .mockLeader(rollClient, 0)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1, 2))
                .addTopic("topic-B", 1, List.of(1, 2, 0), List.of(1, 2, 0))
                .addTopic("topic-C", 2, List.of(2, 0, 1), List.of(2, 0, 1))
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                // The order we expect is combined nodes and active controller
                assertRestartedNodesOrder(1, 2, 0);

                // Verify brokers had leader election
                for (int nodeId : List.of(0, 1, 2)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    void shouldRestartBrokerWithNoTopicIfReasonManualRolling() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .mockHealthyNodes(platformClient, 0)
                .mockSuccessfulConnection(rollClient, 0)
                .mockDescribeConfigs(rollClient, Set.of(), 0)
                .done().get(0);

        doRollingRestart(platformClient, rollClient, null, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder(0);

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
                Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
                Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
            }).get();
    }

    @Test
    public void shouldRestartUnreadyWithManualRollingUpdate() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY), 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.NOT_READY,  PlatformClient.NodeState.READY), 4)
                .mockHealthyNodes(platformClient, 0, 1, 3)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                // The order we expect is unready controller (in this case combined), ready controller, active controller, unready broker, ready broker
                assertRestartedNodesOrder(2, 0, 1, 4, 3);

                // Verify brokers had leader election
                for (int nodeId : List.of(2, 3, 4)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartNotRunningNodes() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockNotRunningNodes(platformClient, 0, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 4)
                .mockHealthyNodes(platformClient, 1, 3)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::podHasOldRevision, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                // The order we expect parallel restart of controllers and then broker, then healthy nodes
                assertRestartedNodesOrder(0, 2, 4, 1, 3);

                // Verify brokers had leader election
                for (int nodeId : List.of(2, 3, 4)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartUnresponsiveNodes() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 1, 3)
                .mockConnectionToNode(rollClient, List.of(false, true), 0)
                .mockConnectionToNode(rollClient, List.of(false, false, true), 2)
                .mockConnectionToNode(rollClient, List.of(false, false, false, true), 4)
                .mockDescribeConfigs(rollClient, Set.of(), 1, 3)
                .done();

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                // The order we expect parallel unresponsive controller, combined node and broker
                assertRestartedNodesOrder(0, 2, 4);

                // Verify broker had leader election
                Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(2)));

                // Verify unresponsive nodes were restarted
                for (int nodeId : List.of(0, 2, 4)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                // Verify responsive nodes were not restarted
                for (int nodeId : List.of(1, 3)) {
                    Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartNonDynamicConfig() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("auto.create.topics.enable", "true")), 0, 1, 2, 3, 4)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doRollingRestart(platformClient, rollClient, null, nodeRefs.values(), RackRollingTest::noReasons, nodeId -> "auto.create.topics.enable=false", 1)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder(0, 2, 1, 3, 4);

                // Verify brokers had leader election
                for (int nodeId : List.of(2, 3, 4)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                for (var nodeRef : nodeRefs.values()) {
                    Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                }
            }).get();
    }

    @Test
    public void shouldRestartBrokerDynamicConfigFailed() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        doThrow(new RuntimeException("Configuration update failed")).when(rollClient).reconfigureNode(any(), any(), anyBoolean());

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("min.insync.replicas", "1")), 0, 1, 2, 3, 4)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doRollingRestart(platformClient,
                rollClient,
                null,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                nodeId -> "min.insync.replicas=2",
                1
        ).whenComplete((r, e) -> {
            assertRestartedNodesOrder(1);

            for (var nodeRef : nodeRefs.values()) {
                Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
                if (nodeRef.broker()) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
                }
            }
        }).get();
    }

    @Test
    void shouldRestartWhenCannotGetBrokerState() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doThrow(new RuntimeException("Cannot get broker state"))
                .when(agentClient)
                .getBrokerState(any());

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder(0, 2, 1, 3, 4);

                // Verify brokers had leader election
                for (int nodeId : List.of(2, 3, 4)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartTwoNodesQuorumControllers() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder(2, 1);

                // Verify controllers were restarted
                for (int nodeId : List.of(1, 2)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                // Verify controllers did not have leader election
                for (int nodeId : List.of(1, 2)) {
                    Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }

    @Test
    public void shouldRestartTwoNodesQuorumOneControllerBehind() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 7_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class, () ->
                doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3).get());

        assertEquals("Cannot restart nodes [pool-kafka-1/1] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.", ex.getMessage());

        // Only the controller that has fallen behind should be restarted
        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    public void shouldRestartSingleNodeQuorum() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder(1, 2);

                // Verify broker had leader election
                Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(2)));

                // Verify controller did not have leader election
                Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));

                // Verify all nodes were restarted
                for (int nodeId : List.of(1, 2)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            }).get();
    }


    //////////////////////////////////////////////////////
    /// Test scenarios we expect no restarts           ///
    //////////////////////////////////////////////////////

    @Test
    void shouldNotRestartNodesNoReason() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockTopics(rollClient)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2, 3, 4)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        doRollingRestart(platformClient,
                rollClient,
                null,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                EMPTY_CONFIG_SUPPLIER,
                1
        ).whenComplete((r, e) -> {
            assertRestartedNodesOrder();

            Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
            Mockito.verify(platformClient, never()).restartNode(any(), any());
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
        }).get();
    }

    @Test
    void shouldThrowUnrestartableNodesExceptionWhenNotReadyAfterRestart() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY, PlatformClient.NodeState.NOT_READY), 0)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2)
                .done();

        var ex = assertThrows(RetriableException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("Timed out waiting for restarted pod pool-kafka-0 to become ready", ex.getCause().getMessage());

        assertRestartedNodesOrder(0);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldThrowUnrestartableNodesExceptionWhenNotReadyNoReason() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 0)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2)
                .done();

        var ex = assertThrows(RetriableException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::noReasons,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("Timed out waiting for non-restarted pod pool-kafka-0 to become ready", ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldThrowUnrestartableNodesExceptionWhenNotReadyWithReason() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 0)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2)
                .done();

        var ex = assertThrows(RetriableException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::podUnresponsive,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("Timed out waiting for restarted pod pool-kafka-0 to become ready", ex.getCause().getMessage());

        assertRestartedNodesOrder(0);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), anyBoolean());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldNotRestartNotRunningNodeWhenDoesNotHaveOldRevision() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1, 2), 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING), 0)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), 0)
                .done();

        var ex = assertThrows(RetriableException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::podUnresponsive,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("Pod pool-kafka-0 is unschedulable or is not starting", ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(2)));
    }

    @Test
    void shouldNotRestartWhenQuorumCheckFailed() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 6000L);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, false, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("controller.quorum.fetch.timeout.ms", "3000")), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The active controller and the up-to-date follower should not be restarted but fallen-behind follower can be restarted as doesn't impact the quorum health
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.", ex.getCause().getMessage());

        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartWhenAvailabilityCheckFailed() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1), 2)
                .mockTopics(rollClient)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The partition leader and in sync replica should not be restarted but out of sync replica can be restarted as doesn't impact the availability
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.", ex.getCause().getMessage());

        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartCombinedNodesWhenQuorumCheckFailed() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L); //default fetch timeout is 2000L
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0)
                .addNodes(platformClient, true, true, 1)
                .addNodes(platformClient, true, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1, 2), 2)
                .mockTopics(rollClient)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The partition leader and in sync replica should not be restarted but out of sync replica can be restarted as doesn't impact the availability
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.", ex.getCause().getMessage());

        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartCombinedNodesWhenAvailabilityCheckFailed() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0)
                .addNodes(platformClient, true, true, 1)
                .addNodes(platformClient, true, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1), 2)
                .mockTopics(rollClient)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // The partition leader and in sync replica should not be restarted but out of sync replica can be restarted as doesn't impact the availability
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.", ex.getCause().getMessage());

        assertRestartedNodesOrder(2);
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    void shouldNotRestartBrokerNodeInRecovery() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0)
                .addNodes(platformClient, false, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockHealthyNodes(platformClient, 0, 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 2)
                .done();

        var bs = BrokerState.RECOVERY;
        bs.setRemainingLogsToRecover(100);
        bs.setRemainingSegmentsToRecover(300);
        doReturn(bs)
                .when(agentClient)
                .getBrokerState(nodeRefs.get(2));

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("The max attempts (3) to wait for this node pool-kafka-2/2 to finish performing log recovery has been reached. There are 100 logs and 300 segments left to recover.",
                ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
    }

    @Test
    void shouldNotRestartControllerNodeInRecovery() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, false, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 2)
                .done();

        var bs = BrokerState.RECOVERY;
        bs.setRemainingLogsToRecover(100);
        bs.setRemainingSegmentsToRecover(300);
        doReturn(bs)
                .when(agentClient)
                .getBrokerState(nodeRefs.get(2));

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("The max attempts (3) to wait for this node pool-kafka-2/2 to finish performing log recovery has been reached. There are 100 logs and 300 segments left to recover.",
                ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
    }

    @Test
    public void shouldNotRestartEvenSizedQuorumTwoControllersBehind() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L, 3, 6000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2, 4) //combined nodes
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1, pool-kafka-2/2, pool-kafka-4/4] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.",
                ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
    }

    @Test
    public void shouldNotRestartControllersWithInvalidTimestamp() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, -1L, 1, 10_000L, 2, -1L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1, pool-kafka-2/2] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.",
                ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
    }

    @Test
    public void shouldNotRollControllersWithInvalidLeader() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, -1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockSuccessfulConnection(rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .done();

        //TODO: Revise how we should handle this (should be similar to the current roller)

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        // we should not restart any controllers as the majority have not caught up to the leader
        assertEquals("Cannot restart nodes [pool-kafka-0/0, pool-kafka-1/1, pool-kafka-2/2] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.",
                ex.getCause().getMessage());

        assertRestartedNodesOrder();
        Mockito.verify(platformClient, never()).restartNode(any(), any());
    }

    @Test
    public void shouldThrowExceptionInitAdminException() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        doThrow(new RuntimeException("Failed to create admin client for brokers")).when(rollClient).initialiseBrokerAdmin(any());
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockSuccessfulConnection(rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .done();

        var ex = assertThrows(RuntimeException.class, () ->
                doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3).get());

        assertEquals("Failed to create admin client for brokers", ex.getCause().getMessage());
    }

    @Test
    public void shouldNotReconfigureWhenAllowReconfigurationIsFalse() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 1)
                .mockHealthyNodes(platformClient, 1)
                .mockSuccessfulConnection(rollClient, 1)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("compression.type", "zstd")), 1)
                .mockTopics(rollClient)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                serverId -> "compression.type=snappy",
                false,
                3);

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::noReasons, serverId -> "compression.type=snappy", 3)
            .whenComplete((r, e) -> {
                assertRestartedNodesOrder();

                Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRefs.get(1)), any(), anyBoolean());
                Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
                Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));
            }).get();
    }

    @Test
    public void shouldNotRestartDynamicConfig() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("min.insync.replicas", "1")), 0, 1, 2, 3, 4)
                .mockReconfigureConfigs(rollClient)
                .done();

        doRollingRestart(platformClient,
                rollClient,
                null,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                nodeId -> "min.insync.replicas=2",
                1
        ).whenComplete((r, e) -> {
            assertRestartedNodesOrder();

            for (var nodeRef : nodeRefs.values()) {
                Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
            }
        }).get();
    }

    @Test
    public void shouldNotRestartDescribeConfigFailed() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        doThrow(new KafkaException("Error getting Kafka config")).when(rollClient).describeBrokerConfigs(any());
        doThrow(new KafkaException("Error getting Kafka config")).when(rollClient).describeControllerConfigs(any());

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, false, 1)
                .addNodes(platformClient, true, true, 2)
                .addNodes(platformClient, false, true, 3)
                .addNodes(platformClient, false, true, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 2, 3, 4)
                .done();

        var ex = assertThrows(KafkaException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        null,
                        nodeRefs.values(),
                        RackRollingTest::noReasons,
                        EMPTY_CONFIG_SUPPLIER,
                        1
                ).get());

        assertEquals("Error getting Kafka config", ex.getCause().getMessage());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), anyBoolean());
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }


    //////////////////////////////////////////////////////
    /// Test scenarios for batch rolling               ///
    //////////////////////////////////////////////////////

    @Test
    public void shouldRestartInExpectedOrderAndBatched() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
            .whenComplete((r, e) -> {
                // The expected order is non-active controllers, active controller and batches of brokers that don't have partitions in common
                assertRestartedNodesOrder(0, 2, 1, 5, 8, 3, 6, 4, 7);

                // Verify brokers had leader election
                for (int nodeId : List.of(3, 4, 5, 6, 7, 8)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(0, 1, 2, 3, 4, 5, 6, 7, 8)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                for (var nodeRef : nodeRefs.values()) {
                    Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                }
            }).get();
    }

    @Test
    public void shouldRestartCombinedNodesInExpectedOrderAndBatched() throws ExecutionException, InterruptedException {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
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
                .mockLeader(rollClient, 3)
                .mockHealthyNodes(platformClient, 3, 4, 5, 6, 7, 8)
                .mockSuccessfulConnection(rollClient, 3, 4, 5, 6, 7, 8)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3)
            .whenComplete((r, e) -> {
                // The expected order to restart nodes individually based on the availability and quorum health and then the broker that is the active controller will be started at last
                assertRestartedNodesOrder(6, 4, 7, 5, 8, 3);

                // Verify brokers had leader election
                for (int nodeId : List.of(3, 4, 5, 6, 7, 8)) {
                    Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
                }

                // Verify all nodes were restarted
                for (int nodeId : List.of(3, 4, 5, 6, 7, 8)) {
                    Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
                }

                for (var nodeRef : nodeRefs.values()) {
                    Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), anyBoolean());
                }
            }).get();
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatchedWithUrp() {
        clearRestartOrder();
        PlatformClient platformClient = mockedPlatformClient();
        RollClient rollClient = mockedRollClient();
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockSuccessfulConnection(rollClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                // topic A is at its min ISR, so neither 3 nor 4 should be restarted
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4), 2)
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, defaultQuorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        var ex = assertThrows(MaxAttemptsExceededException.class,
                () -> doRollingRestart(platformClient,
                        rollClient,
                        agentClient,
                        nodeRefs.values(),
                        RackRollingTest::manualRolling,
                        EMPTY_CONFIG_SUPPLIER,
                        3
                ).get());

        assertEquals("Cannot restart nodes [pool-kafka-3/3, pool-kafka-4/4] because they violate quorum health or topic availability. The max attempts (3) to retry the nodes has been reached.",
                ex.getCause().getMessage());

        // The expected order is non-active controller nodes, the active controller,
        // batches of brokers starting with the largest.
        assertRestartedNodesOrder(0, 2, 1, 5, 8, 7, 6);

        // Verify controllers were restarted
        for (int nodeId : List.of(0, 1, 2)) {
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
        }

        // Verify some brokers were restarted
        for (int nodeId : List.of(5, 6, 7, 8)) {
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
        }

        // Verify nodes 3 and 4 were not restarted due to availability constraints
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(3)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(4)), any());
    }
}
