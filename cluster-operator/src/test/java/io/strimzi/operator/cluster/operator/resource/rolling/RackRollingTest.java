/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
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

    static RestartReasons noReasons(int serverId) {
        return RestartReasons.empty();
    }

    private static RestartReasons manualRolling(int serverId) {
        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
    }

    private static RestartReasons podUnresponsive(int serverId) {
        return RestartReasons.of(RestartReason.POD_UNRESPONSIVE);
    }

    private static RestartReasons podHasOldRevision(int serverId) {
        return RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION);
    }

    static class MockBuilder {
        private final Map<Integer, NodeRef> nodeRefs = new HashMap<>();
        private final Map<Integer, Node> nodes = new HashMap<>();
        private final Set<TopicListing> topicListing = new HashSet<>();
        private final Map<String, Integer> topicMinIsrs = new HashMap<>();
        private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();

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

        MockBuilder mockBrokerState(AgentClient agentClient, List<BrokerState> brokerStates, int nodeId) {
            doReturn(brokerStates.get(0), brokerStates.size() == 1 ? new Object[0] : brokerStates.subList(1, brokerStates.size()).toArray())
                    .when(agentClient)
                    .getBrokerState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockHealthyNodes(PlatformClient platformClient, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockHealthyNode(platformClient, nodeId);
            }
            return this;
        }

        MockBuilder mockHealthyNode(PlatformClient platformClient, int nodeId) {
            doReturn(PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockUnhealthyNode(PlatformClient platformClient, int nodeId) {
            doReturn(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockPermanentlyUnhealthyNode(PlatformClient platformClient, int nodeId) {
            doReturn(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY,
                    PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY,
                    PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY,
                    PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder addTopic(String topicName, int leaderId) {
            return addTopic(topicName, leaderId, List.of(leaderId), List.of(leaderId), null);
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

        MockBuilder mockDescribeConfigs(RollClient rollClient, Set<ConfigEntry> nodeConfigs, Set<ConfigEntry> loggerConfigs, int... nodeIds) {
            Map<Integer, Configs> configPair = new HashMap<>();
            for (var nodeId : nodeIds) {
                configPair.put(nodeId, new Configs(new Config(nodeConfigs), new Config(loggerConfigs)));
            }
            doReturn(configPair)
                    .when(rollClient)
                    .describeBrokerConfigs(any());
            doReturn(configPair)
                    .when(rollClient)
                    .describeControllerConfigs(any());
            return this;
        }

        MockBuilder mockDescribeConfigsWithUpdatedResult(RollClient rollClient, Configs currentConfigs, Configs updatedConfigs, int nodeId) {
            when(rollClient.describeControllerConfigs(any()))
                    .thenReturn(Map.of(nodeId, currentConfigs))
                    .thenReturn(Map.of(nodeId, updatedConfigs));

            when(rollClient.describeBrokerConfigs(any()))
                    .thenReturn(Map.of(nodeId, currentConfigs))
                    .thenReturn(Map.of(nodeId, updatedConfigs));
            return this;
        }

        MockBuilder mockQuorumLastCaughtUpTimestamps(RollClient rollClient, Map<Integer, Long> quorumState) {
            doReturn(quorumState)
                    .when(rollClient)
                    .quorumLastCaughtUpTimestamps();
            return this;
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int nodeId) {
            return mockElectLeaders(rollClient, List.of(0), nodeId);
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int... nodeIds) {
            return mockElectLeaders(rollClient, List.of(0), nodeIds);
        }

        MockBuilder mockCanConnectToNodes(RollClient rollClient, boolean canConnect, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockCanConnectToNode(rollClient, canConnect, nodeId);
            }
            return this;
        }

        public MockBuilder mockCanConnectToNode(RollClient rollClient, boolean canConnect, int nodeId) {
            doReturn(canConnect).when(rollClient).canConnectToNode(eq(nodeRefs.get(nodeId)), anyBoolean());
            return this;
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, List<Integer> results, int... nodeIds) {
            for (var nodeId : nodeIds) {
                doReturn(results.get(0), results.subList(1, results.size()).toArray())
                        .when(rollClient)
                        .tryElectAllPreferredLeaders(nodeRefs.get(nodeId));
            }
            return this;
        }

        public Map<Integer, NodeRef> done() {
            return Map.copyOf(nodeRefs);
        }

        public MockBuilder mockLeader(RollClient rollClient, int leaderId) {
            doReturn(leaderId).when(rollClient).activeController();
            return this;
        }
    }


    private static void assertNodesRestarted(PlatformClient platformClient,
                                             RollClient rollClient,
                                             Map<Integer, NodeRef> nodeRefs,
                                             RackRolling rr,
                                             int... nodeIds) throws TimeoutException, InterruptedException, ExecutionException {
        for (var nodeId : nodeIds) {
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(nodeId)), any());
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));

        }
        List<Integer> restartedNodes = rr.loop();
        List<Integer> expectedRestartedNodes = IntStream.of(nodeIds).boxed().toList();
        assertEquals(expectedRestartedNodes.size(), restartedNodes.size());
        assertTrue(restartedNodes.containsAll(expectedRestartedNodes));
        for (var nodeId : nodeIds) {
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)), any());
            if  (nodeRefs.get(nodeId).broker()) {
                Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
            }
        }
    }

    private RackRolling newRollingRestart(PlatformClient platformClient,
                                RollClient rollClient,
                                AgentClient agentClient,
                                Collection<NodeRef> nodeRefList,
                                Function<Integer, RestartReasons> reason,
                                Function<Integer, String> kafkaConfigProvider,
                                boolean allowReconfiguration,
                                int maxRestartsBatchSize) {
        return RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                agentClient,
                nodeRefList,
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                nodeRefList.stream().filter(NodeRef::controller).collect(Collectors.toList()).size(),
                KafkaVersionTestUtils.getLatestVersion(),
                allowReconfiguration,
                kafkaConfigProvider,
                null,
                120_000,
                0,
                maxRestartsBatchSize,
                1,
                1,
                1);
    }


    private void doRollingRestart(PlatformClient platformClient,
                                  RollClient rollClient,
                                  AgentClient agentClient,
                                  Collection<NodeRef> nodeRefList,
                                  Function<Integer, RestartReasons> reason,
                                  Function<Integer, String> kafkaConfigProvider,
                                  String desiredLogging,
                                  int maxRestartsBatchSize,
                                  int maxRestart) throws ExecutionException, InterruptedException, TimeoutException {

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                agentClient,
                nodeRefList,
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                nodeRefList.stream().filter(NodeRef::controller).collect(Collectors.toList()).size(),
                KafkaVersionTestUtils.getLatestVersion(),
                true,
                kafkaConfigProvider,
                desiredLogging,
                120_000,
                0,
                maxRestartsBatchSize,
                maxRestart,
                1,
                2);
        List<Integer> nodes;
        do {
            nodes = rr.loop();
        } while (!nodes.isEmpty());
    }

    @Test
    void shouldNotRestartBrokersWithNoTopicsIfAllHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockHealthyNode(platformClient, 0)
                .mockCanConnectToNode(rollClient, true, 0)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, null, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, never()).restartNode(any(), any());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldRestartBrokerWithNoTopicIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .mockHealthyNode(platformClient, 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);

        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .mockHealthyNode(platformClient, 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowMaxRestartsExceededIfBrokerRestartsMoreThanMaxRestarts() {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .addTopic("topic-A", 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        var ex = assertThrows(MaxRestartsExceededException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, null, 1, 1));

        //then
        assertEquals("Node pool-kafka-0/0 has been restarted 1 times", ex.getMessage());
    }

    @Test
    void shouldRestartIfMaxReconfigExceeded() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRef = new MockBuilder()
                .addNode(platformClient, true, true, 0)
                .mockHealthyNode(platformClient, 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("compression.type", "zstd")), Set.of(), 0)
                .done().get(0);

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                Collections.singleton(nodeRef),
                RackRollingTest::noReasons,
                serverId -> "compression.type=snappy",
                true,
                3);

        // Attempt until the maxReconfig of 3 is exceeded and then node is restarted
        for (int i = 0; i < 5; i++) {
            rr.loop();
        }

        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
    }

    @Test
    void shouldNotThrowExceptionIfAllPreferredLeaderNotElectedAfterRestart() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .addNode(platformClient, true, false, 1)
                .mockHealthyNodes(platformClient, 0, 1)
                .mockCanConnectToNodes(rollClient, true, 0, 1)
                .addTopic("topic-A", 0)
                .mockLeader(rollClient, 1)
                .mockQuorumLastCaughtUpTimestamps(rollClient, Map.of(1, 10_000L))
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1)
                .mockElectLeaders(rollClient,
                        List.of(2), // there's always 2 preferred partitions which broker 0 cannot be elected leader of
                        0)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 1, 1);

        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRefs.get(0)), any(), any());
        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRefs.get(1)), any(), any());

        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(1)), any());

        Mockito.verify(rollClient, atLeastOnce()).tryElectAllPreferredLeaders(eq(nodeRefs.get(0)));
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));

    }

    @Test
    void shouldNotThrowExceptionIfAllPreferredLeadersNotElectedAfterReconfig() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .addNode(platformClient, true, false, 1)
                .mockHealthyNodes(platformClient, 0, 1)
                .mockCanConnectToNodes(rollClient, true, 0, 1)
                .addTopic("topic-A", 0)
                .mockLeader(rollClient, 1)
                .mockQuorumLastCaughtUpTimestamps(rollClient, Map.of(1, 10_000L))
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("compression.type", "zstd")), Set.of(), 0, 1)
                .mockElectLeaders(rollClient, List.of(1), 0)
                .done();

        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::noReasons, serverId -> "compression.type=snappy", null, 1, 1);

        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRefs.get(0)), any(), any());
        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRefs.get(1)), any(), any());

        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(0)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());

        Mockito.verify(rollClient, atLeastOnce()).tryElectAllPreferredLeaders(eq(nodeRefs.get(0)));
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));
    }

    @Test
    void shouldRepeatAllPreferredLeaderElectionCallsUntilAllPreferredLeaderElected() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .addTopic("topic-A", 0)
                .mockHealthyNode(platformClient, 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, List.of(1, 1, 1, 1, 0), 0)
                .done().get(0);

        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, null, 1, 1);

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
        Mockito.verify(rollClient, times(5)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowExceptionIfNodeNotAbleToRecoverAfterRestart() {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .addTopic("topic-A", 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY, PlatformClient.NodeState.NOT_READY), 0)
                .mockBrokerState(agentClient, List.of(BrokerState.RECOVERY), 0)
                .mockTopics(rollClient)
                .done().get(0);

        var te = assertThrows(UnrestartableNodesException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, null, 1, 3));

        assertEquals("The max attempts (2) to wait for this node pool-kafka-0/0 to finish performing log recovery has been reached. There are 0 logs and 0 segments left to recover.",
                te.getMessage());
    }

    @Test
    void shouldThrowExceptionWithRecoveryProgressIfNodeIsInRecovery() {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var bs = BrokerState.RECOVERY;
        bs.setRemainingLogsToRecover(100);
        bs.setRemainingSegmentsToRecover(300);
        doReturn(bs)
                .when(agentClient)
                .getBrokerState(any());

        var nodeRef = new MockBuilder()
                .addNode(platformClient, true, true, 0)
                .addTopic("topic-A", 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 0)
                .mockTopics(rollClient)
                .done().get(0);

        var te = assertThrows(RuntimeException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, null, 1, 1));

        assertEquals("The max attempts (2) to wait for this node pool-kafka-0/0 to finish performing log recovery has been reached. There are 100 logs and 300 segments left to recover.",
                te.getMessage());

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
    }

    @Test
    void shouldNotRestartUnreadyNodes() {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        var te = assertThrows(RuntimeException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, null, 1, 1));

        assertEquals("java.util.concurrent.TimeoutException: Failed to reach SERVING within 120000 ms: Context[nodeRef=pool-kafka-0/0, currentRoles=NodeRoles[controller=false, broker=true], state=NOT_READY, lastTransition=1970-01-01T00:00:00Z, reason=[], numRestarts=0, numReconfigs=0, numAttempts=2]",
                te.getMessage());

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
    }

    @Test
    void shouldRestartUnresponsiveNode() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockCanConnectToNodes(rollClient, false, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                EMPTY_CONFIG_SUPPLIER,
                true,
                1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(0)), any());
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .mockBrokerState(agentClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockDescribeConfigsWithUpdatedResult(rollClient,
                        new Configs(new Config(Set.of(new ConfigEntry("compression.type", "zstd"))), new Config(Set.of())),
                        new Configs(new Config(Set.of(new ConfigEntry("compression.type", "snappy"))), new Config(Set.of())),
                0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "compression.type=snappy", null, 0, 0);

        // then
        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());}

    @Test
    void shouldRestartBrokerIfChangedNonReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockLeader(rollClient, -1)
                .mockCanConnectToNodes(rollClient, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(agentClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient,
                        Set.of(new ConfigEntry("auto.leader.rebalance.enable", "true")), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "auto.leader.rebalance.enable=false", null, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableLoggingParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRef = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(agentClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .mockCanConnectToNodes(rollClient, true, 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigsWithUpdatedResult(rollClient,
                        new Configs(new Config(Set.of()), new Config(Set.of())),
                        new Configs(new Config(Set.of()), new Config(Set.of(new ConfigEntry("org.apache.kafka", "DEBUG"), new ConfigEntry("root", "WARN")))),
                        0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, agentClient, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, "log4j.logger.org.apache.kafka=DEBUG", 1, 1);

        // then
        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
    }

    @Test
    void shouldNotRestartBrokersIfHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .addTopic("topic-0", 0)
                .addTopic("topic-1", 1)
                .addTopic("topic-2", 2)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, null, 3, 1);

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(platformClient, never()).restartNode(any(), any());
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
        }
    }

    @Test
    void shouldRestartBrokersIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, false, true, 0, 1, 2)
                .mockLeader(rollClient, -1)
                .addTopic("topic-0", 0)
                .addTopic("topic-1", 1)
                .addTopic("topic-2", 2)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        doRollingRestart(platformClient, rollClient, agentClient,
                nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1);

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef), any());
            Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }

    @Test
    public void shouldRestartInExpectedOrder() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .addNodes(platformClient, false, true, 3, 4, 5)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2, 3, 4, 5)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 4, List.of(4, 5, 3), List.of(3, 4, 5))
                .addTopic("topic-C", 5, List.of(5, 3, 4), List.of(3, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5)
                .done();

        // when

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // then
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 3);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 5);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 4);

        assertEquals(List.of(), rr.loop());

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartCombinedNodesInExpectedOrder() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .addTopic("topic-A", 0, List.of(0, 1, 2), List.of(0, 1, 2))
                .addTopic("topic-B", 1, List.of(1, 2, 0), List.of(0, 1, 2))
                .addTopic("topic-C", 2, List.of(2, 0, 1), List.of(0, 1, 2))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // then
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertEquals(List.of(), rr.loop());

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatched() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        // when
        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // The expected order is non-active controllers, active controller and batches of brokers that don't have partitions in common
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0); //non-active controller

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2); //non-active controller

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1); //active controller

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 5, 8);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 4, 7);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 3, 6);

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartCombinedNodesInExpectedOrderAndBatched() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
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
                .mockCanConnectToNodes(rollClient, true, 3, 4, 5, 6, 7, 8)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // The expected order to restart nodes individually based on the availability and quorum health and then the broker that is the active controller will be started at last
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 4);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 5);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 6);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 7);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 8);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 3); // the active controller

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatchedWithUrp() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2) // controllers
                .addNodes(platformClient, false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                // topic A is at its min ISR, so neither 3 nor 4 should be restarted
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4), 2)
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // The expected order is non-active controller nodes, the active controller,
        // batches of brokers starting with the largest.
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 5, 8); // the largest batch of brokers that do not have partitions in common

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 6); // 6 doesn't have partitions in common with 3 but 3 will cause under min ISR

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 7); // 7 doesn't have partitions in common with 4 but 4 will cause under min ISR

        // But for the reconciliation to eventually fail because of the unrestartable nodes
        assertThrows(UnrestartableNodesException.class, rr::loop,
                "Expect UnrestartableNodesException because neither broker 3 nor 4 can be restarted while respecting the min ISR on topic A");

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(3)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(4)), any());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartCombinedNodesInExpectedOrderAndBatchedWithUrp() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(3, 10_000L,
                4, 10_000L,
                5, 10_000L,
                6, 10_000L,
                7, 10_000L,
                8, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, // combined nodes
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 6)
                .mockHealthyNodes(platformClient, 3, 4, 5, 6, 7, 8)
                .mockCanConnectToNodes(rollClient, true, 3, 4, 5, 6, 7, 8)
                // topic A is at its min ISR, so neither 3 nor 4 should be restarted
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4), 2)
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 3, 4, 5, 6, 7, 8)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        // when
        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // Expected to restart one node at a time since they are also controllers.
        // 3 and 4 can be restarted because they impact topic availability if restarted.
        // The active controller 6 is not restarted until 3 and 4 can be restarted.
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 5);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 7);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 8);

        // But for the reconciliation to eventually fail because of the unrestartable nodes
        assertThrows(UnrestartableNodesException.class, rr::loop,
                "Expect UnrestartableNodesException because neither broker 3 nor 4 can be restarted while respecting the min ISR on topic A");

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(3)), any());

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(4)), any());

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(6)), any());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRollOddSizedQuorumOneControllerBehind() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        assertThrows(UnrestartableNodesException.class, () ->
                        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1),
                "Expect UnrestartableNodesException because neither controller 0 nor 1 can be restarted without impacting the quorum health");

        // We should be able to restart only the controller that is behind
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldNotRollEvenSizedQuorumTwoControllersBehind() {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L, 3, 6000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2, 4) //combined nodes
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2, 4)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2, 4)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 4)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2, 4)
                .done();

        // we should not restart any controllers as the majority have not caught up to the leader
        assertThrows(UnrestartableNodesException.class, () ->
                doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1),
                "Expect UnrestartableNodesException because none of the controllers can be restarted without impacting the quorum health");

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRollWithCustomControllerFetchTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 7000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("controller.quorum.fetch.timeout.ms", "4000")), Set.of(), 1)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // We should be able to restart all the nodes because controller.quorum.fetch.timeout.ms value was increased
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        //active controller gets restarted last
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldNotRollControllersWithInvalidTimestamp() {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, -1L, 1, 10_000L, 2, -1L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        assertThrows(UnrestartableNodesException.class, () ->
                        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1),
                "Expect UnrestartableNodesException because of invalid timestamps for controller 0 and 2");

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldNotRollControllersWithInvalidLeader() {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(0, 10_000L, 1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0, 1, 2)
                .mockLeader(rollClient, -1)
                .mockHealthyNodes(platformClient, 0, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        assertThrows(UnrestartableNodesException.class, () ->
                        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1),
                "Expect UnrestartableNodesException because of invalid quorum leader");

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRef), any());
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRollTwoNodesQuorumControllers() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollTwoNodesQuorumOneControllerBehind() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L, 2, 7_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        assertThrows(UnrestartableNodesException.class, () ->
                        doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, null, 3, 1),
                "Expect UnrestartableNodesException because of controller 2 has fallen behind therefore controller 1 cannot be restarted");
        //only the controller that has fallen behind should be restarted
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(2)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
    }

    @Test
    public void shouldRollSingleNodeQuorum() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollNodesIfAllNotRunning() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 0)
                .addNodes(platformClient, true, true, 1)
                .addNodes(platformClient, false, true, 2)
                .mockLeader(rollClient, 0)
                .addTopic("topic-A", 0)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 2)
                .mockTopics(rollClient)
                .done();


        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::podHasOldRevision,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // the order we expect are pure controller, combined and broker only
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);
    }

    @Test
    public void shouldRestartCombinedNodesIfAllNotRunning() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 0, 1, 2, 3, 4, 5)
                .addTopic("topic-A", 0)
                // all nodes are combined and not running e.g. pending
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 2)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 3)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 4)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 5)
                .mockCanConnectToNodes(rollClient, true, 0, 1, 2, 3, 4, 5)
                .mockTopics(rollClient)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        // we expect all the combined nodes to be restarted in parallel in order to form the quorum
        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 0, 1, 2, 3, 4, 5);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);
    }

    @Test
    void shouldFailReconciliationIfControllerNodeNeverBecomeReady() {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .addNode(platformClient, true, false, 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING), 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING), 1)
                .mockTopics(rollClient)
                .done();

        var ex = assertThrows(TimeoutException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, null, 1, 3));

        assertEquals("Failed to reach SERVING within 120000 ms: Context[nodeRef=pool-kafka-1/1, currentRoles=NodeRoles[controller=true, broker=false], state=NOT_RUNNING, lastTransition=1970-01-01T00:00:00Z, reason=[POD_STUCK], numRestarts=0, numReconfigs=0, numAttempts=2]", ex.getMessage());

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(0)).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(0)), any());
    }

    @Test
    void shouldFailReconciliationIfBrokerNodeNeverBecomeReady() {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        var nodeRefs = new MockBuilder()
                .addNode(platformClient, false, true, 0)
                .addNode(platformClient, true, false, 1)
                .mockCanConnectToNodes(rollClient, true, 0, 1)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY, PlatformClient.NodeState.NOT_READY), 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY, PlatformClient.NodeState.NOT_RUNNING, PlatformClient.NodeState.READY), 1)
                .mockTopics(rollClient)
                .done();

        //If nodes never got into running state, we have to exit the inner loop somehow.
        var ex = assertThrows(TimeoutException.class,
                () -> doRollingRestart(platformClient, rollClient, agentClient, nodeRefs.values(), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, null, 1, 3));

        assertEquals("Failed to reach SERVING within 120000 ms: Context[nodeRef=pool-kafka-0/0, currentRoles=NodeRoles[controller=false, broker=true], state=NOT_READY, lastTransition=1970-01-01T00:10:19Z, reason=[POD_STUCK], numRestarts=2, numReconfigs=0, numAttempts=2]", ex.getMessage());

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(platformClient, times(2)).restartNode(eq(nodeRefs.get(0)), any());
    }

    @Test
    public void shouldRestartFailedAdminConnection() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockCanConnectToNode(rollClient, true, 1)
                .mockCanConnectToNode(rollClient, false, 2)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::noReasons,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr, 2);
    }

    @Test
    public void shouldNotRestartInitAdminException() {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);
        doThrow(new RuntimeException("Failed to create admin client for brokers")).when(rollClient).initialiseBrokerAdmin();
        Map<Integer, Long> quorumState = Map.of(1, 10_000L);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, false, 1)
                .addNode(platformClient, false, true, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, 1, 2)
                .mockCanConnectToNodes(rollClient, true, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockQuorumLastCaughtUpTimestamps(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = newRollingRestart(platformClient,
                rollClient,
                agentClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                EMPTY_CONFIG_SUPPLIER,
                true,
                3);

        var ex = assertThrows(RuntimeException.class, rr::loop,
                "Expect RuntimeException because admin client cannot  be created");
        assertEquals("Failed to create admin client for brokers", ex.getMessage());
    }

    @Test
    public void testFalseAllowReconfiguration() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        AgentClient agentClient = mock(AgentClient.class);

        var nodeRefs = new MockBuilder()
                .addNodes(platformClient, true, true, 1)
                .mockHealthyNodes(platformClient, 1)
                .mockCanConnectToNodes(rollClient, true, 1)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("compression.type", "zstd")), Set.of(), 1)
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

        assertNodesRestarted(platformClient, rollClient, nodeRefs, rr);

        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRefs.get(1)), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(1)), any());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(1)));
    }
}
