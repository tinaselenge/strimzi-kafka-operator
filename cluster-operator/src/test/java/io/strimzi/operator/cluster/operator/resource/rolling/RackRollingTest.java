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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class RackRollingTest {

    // TODO Tests for combined-mode clusters
    // TODO Tests for ZooKeeper-like clusters
    // TODO handling of exceptions from the admin client
    // TODO Tests for pending nodes

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

    static class MockBuilder {
        private final Map<Integer, NodeRef> nodeRefs = new HashMap<>();
        private final Map<Integer, Node> nodes = new HashMap<>();
        private final Set<TopicListing> topicListing = new HashSet<>();
        private final Map<String, Integer> topicMinIsrs = new HashMap<>();
        private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();

        MockBuilder addNode(boolean controller, boolean broker, int nodeId) {
            if (nodeRefs.containsKey(nodeId)) {
                throw new RuntimeException();
            }
            if (!controller && !broker) {
                throw new RuntimeException();
            }
            nodeRefs.put(nodeId, new NodeRef("pool-kafka-" + nodeId, nodeId, "pool", controller, broker));
            nodes.put(nodeId, new Node(nodeId, "pool-kafka-" + nodeId, 9092));
            return this;
        }

        MockBuilder addNodes(boolean controller, boolean broker, int... nodeIds) {
            for (int nodeId : nodeIds) {
                addNode(controller, broker, nodeId);
            }
            return this;
        }

        MockBuilder mockNodeState(PlatformClient platformClient, List<PlatformClient.NodeState> nodeStates, int nodeId) {
            doReturn(nodeStates.get(0), nodeStates.size() == 1 ? new Object[0] : nodeStates.subList(1, nodeStates.size()).toArray())
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockBrokerState(RollClient rollClient, List<BrokerState> brokerStates, int nodeId) {
            doReturn(brokerStates.get(0), brokerStates.size() == 1 ? new Object[0] : brokerStates.subList(1, brokerStates.size()).toArray())
                    .when(rollClient)
                    .getBrokerState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockHealthyNodes(PlatformClient platformClient, RollClient rollClient, int... nodeIds) {
            for (var nodeId : nodeIds) {
                mockHealthyNode(platformClient, rollClient, nodeId);
            }
            return this;
        }

        MockBuilder mockHealthyNode(PlatformClient platformClient, RollClient rollClient, int nodeId) {
            doReturn(PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            doReturn(BrokerState.RUNNING)
                    .when(rollClient)
                    .getBrokerState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockUnhealthyNode(PlatformClient platformClient, RollClient rollClient, int nodeId) {
            doReturn(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            doReturn(BrokerState.NOT_RUNNING, BrokerState.RUNNING)
                    .when(rollClient)
                    .getBrokerState(nodeRefs.get(nodeId));
            return this;
        }

        MockBuilder mockPermanentlyUnhealthyNode(PlatformClient platformClient, RollClient rollClient, int nodeId) {
            doReturn(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY,
                    PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY,
                    PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY)
                    .when(platformClient)
                    .nodeState(nodeRefs.get(nodeId));
            doReturn(BrokerState.NOT_RUNNING, BrokerState.RUNNING,
                    BrokerState.NOT_RUNNING, BrokerState.RUNNING,
                    BrokerState.NOT_RUNNING, BrokerState.RUNNING)
                    .when(rollClient)
                    .getBrokerState(nodeRefs.get(nodeId));
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
            Map<Integer, RollClient.Configs> configPair = new HashMap<>();
            for (var nodeId : nodeIds) {
                configPair.put(nodeId, new RollClient.Configs(new Config(nodeConfigs), new Config(loggerConfigs)));
            }
            doReturn(configPair)
                    .when(rollClient)
                    .describeKafkaConfigs(any());
            return this;
        }

        MockBuilder mockDescribeQuorum(RollClient rollClient, Map<Integer, Long> quorumState) {
            doReturn(quorumState)
                    .when(rollClient)
                    .describeQuorumState();
            return this;
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int nodeId) {
            return mockElectLeaders(rollClient, List.of(0), nodeId);
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int... nodeIds) {
            return mockElectLeaders(rollClient, List.of(0), nodeIds);
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


    private static void assertBrokerRestarted(PlatformClient platformClient,
                                              RollClient rollClient,
                                              Map<Integer, NodeRef> nodeRefs,
                                              RackRolling rr,
                                              int... nodeIds) throws TimeoutException, InterruptedException, ExecutionException {
        for (var nodeId : nodeIds) {
            Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(nodeId)));
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
        }
        assertEquals(IntStream.of(nodeIds).boxed().toList(), rr.loop());
        for (var nodeId : nodeIds) {
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRefs.get(nodeId)));
            Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRefs.get(nodeId)));
        }
    }


    private void doRollingRestart(PlatformClient platformClient,
                                  RollClient rollClient,
                                  Collection<NodeRef> nodeRefList,
                                  Function<Integer, RestartReasons> reason,
                                  Function<Integer, String> kafkaConfigProvider,
                                  Integer maxRestartsBatchSize,
                                  Integer maxRestarts) throws ExecutionException, InterruptedException, TimeoutException {

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefList,
                reason,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                kafkaConfigProvider,
                null,
                30_000,
                120_000,
                maxRestartsBatchSize,
                maxRestarts);
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
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockHealthyNode(platformClient, rollClient, 0)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, never()).restartNode(any());
        Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
    }

    @Test
    void shouldRestartBrokerWithNoTopicIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockHealthyNode(platformClient, rollClient, 0)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockHealthyNode(platformClient, rollClient, 0)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowMaxRestartsExceededIfBrokerRestartsMoreThanMaxRestarts() {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .addTopic("topic-A", 0)
                .mockPermanentlyUnhealthyNode(platformClient, rollClient, 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        var ex = assertThrows(MaxRestartsExceededException.class,
                () -> doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 1));

        //then
        assertEquals("Broker 0 has been restarted 1 times", ex.getMessage());
    }

    @Test
    void shouldThrowTimeoutExceptionIfAllPreferredLeaderNotElected() {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .addTopic("topic-A", 0)
                .mockUnhealthyNode(platformClient, rollClient, 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient,
                        List.of(2), // there's always 2 preferred partitions which broker 0 cannot be elected leader of
                        0)
                .done().get(0);

        var te = assertThrows(TimeoutException.class,
                () -> doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2));

        assertEquals("Failed to reach LEADING_ALL_PREFERRED within 117000: " +
                        "Context[nodeRef=pool-kafka-0/0, state=SERVING, " +
                        "lastTransition=1970-01-01T00:00:03Z, reason=[POD_UNRESPONSIVE], numRestarts=1]",
                te.getMessage());

    }

    @Test
    void shouldThrowTimeoutExceptionIfAllPreferredLeadersNotElected() {
        // TODO how does this test differ from the one above?
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockHealthyNode(platformClient, rollClient, 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("compression.type", "zstd")), Set.of(), 0)
                .mockElectLeaders(rollClient, List.of(1), 0)
                .done().get(0);

        // when
        var te = assertThrows(TimeoutException.class,
                () -> doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "compression.type=snappy", 1, 1));

        // then
        assertEquals("Failed to reach LEADING_ALL_PREFERRED within 15000: " +
                        "Context[nodeRef=pool-kafka-0/0, state=RECONFIGURED, " +
                        "lastTransition=1970-01-01T00:00:00Z, " +
                        "reason=[], numRestarts=0]",
                te.getMessage());
    }

    @Test
    void shouldRepeatAllPreferredLeaderElectionCallsUntilAllPreferredLeaderElected() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .addTopic("topic-A", 0)
                .mockHealthyNode(platformClient, rollClient, 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, List.of(1, 1, 1, 1, 0), 0)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .done().get(0);

        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2);

        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(5)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowTimeoutExceptionIfPodNotAbleToRecoverAfterRestart() {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .addTopic("topic-A", 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(rollClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING), 0)
                .mockTopics(rollClient)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .done().get(0);

        var te = assertThrows(TimeoutException.class,
                () -> doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2));

        assertEquals("Failed to reach SERVING within 120000 ms: " +
                        "Context[nodeRef=pool-kafka-0/0, state=RECOVERING, " +
                        "lastTransition=1970-01-01T00:00:01Z, reason=[POD_UNRESPONSIVE], numRestarts=1]",
                te.getMessage());
    }

    @Test
    void shouldRestartNotReadyBrokerEvenIfNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.NOT_READY, PlatformClient.NodeState.READY), 0)
                .mockBrokerState(rollClient, List.of(BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(rollClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockDescribeConfigs(rollClient,
                        Set.of(new ConfigEntry("compression.type", "zstd")),
                        Set.of(),
                        0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "compression.type=snappy", 1, 1);

        // then
        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));

    }

    @Test
    void shouldRestartBrokerIfChangedNonReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(rollClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient,
                        Set.of(new ConfigEntry("auto.leader.rebalance.enable", "true")), Set.of(), 0)
                .mockElectLeaders(rollClient, 0)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "auto.leader.rebalance.enable=false", 1, 1);

        // then
        Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableLoggingParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockNodeState(platformClient, List.of(PlatformClient.NodeState.READY), 0)
                .mockBrokerState(rollClient, List.of(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING), 0)
                .addTopic("topic-A", 0)
                .mockTopics(rollClient)
                .mockDescribeConfigs(rollClient,
                        Set.of(),
                        Set.of(new ConfigEntry("org.apache.kafka", "DEBUG")), 0)
                .mockElectLeaders(rollClient, 0)
                .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::noReasons, serverId -> "log.retention.ms=1000", 1, 1);

        // then
        Mockito.verify(rollClient, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRef));
        Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldNotRestartBrokersIfHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(false, true, 0, 1, 2)
                .addTopic("topic-0", 0)
                .addTopic("topic-1", 1)
                .addTopic("topic-2", 2)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        doRollingRestart(platformClient, rollClient, nodeRefs.values(), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 3, 5);

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(platformClient, never()).restartNode(any());
            Mockito.verify(rollClient, never()).tryElectAllPreferredLeaders(any());
        }
    }

    @Test
    void shouldRestartBrokersIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRefs = new MockBuilder()
                .addNodes(false, true, 0, 1, 2)
                .addTopic("topic-0", 0)
                .addTopic("topic-1", 1)
                .addTopic("topic-2", 2)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockDescribeQuorum(rollClient, Map.of(0, 10000L))
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        doRollingRestart(platformClient, rollClient, nodeRefs.values(),
                RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3, 1);

        // then
        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
            Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }

    @Test
    public void shouldRestartInExpectedOrder() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2)
                .addNodes(false, true, 3, 4, 5)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2, 3, 4, 5)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 4, List.of(4, 5, 3), List.of(3, 4, 5))
                .addTopic("topic-C", 5, List.of(5, 3, 4), List.of(3, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5)
                .done();

        // when

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // then
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 4);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 5);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 3);

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
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2) // controllers
                .addNodes(false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4, 5))
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        // when

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // then
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 3, 6);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 4, 7);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 5, 8);

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldRestartInExpectedOrderAndBatchedWithUrp() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2) // controllers
                .addNodes(false, true, // brokers
                        3, 6, // rack X
                        4, 7, // rack Y
                        5, 8) // rack Z
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2, 3, 4, 5, 6, 7, 8)
                // topic A is at its min ISR, so neither 3 nor 4 should be rolled
                .addTopic("topic-A", 3, List.of(3, 4, 5), List.of(3, 4), 2)
                .addTopic("topic-B", 6, List.of(6, 7, 8), List.of(6, 7, 8))
                .addTopic("topic-C", 4, List.of(4, 8, 6), List.of(4, 8, 6))
                .addTopic("topic-D", 7, List.of(7, 3, 5), List.of(7, 3, 5))
                .addTopic("topic-E", 6, List.of(6, 4, 5), List.of(6, 4, 5))
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 3, 4, 5, 6, 7, 8)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 3, 4, 5, 6, 7, 8)
                .done();

        // when
        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // then
        // We expect all the nodes which can be restarted to have been restarted
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 7);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 6);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 5, 8);

        // TODO should we fail fast (after other nodes have been restarted)
        //  or wait for some amount of time in case those brokers not in the replicas rejoin?

        // But for the reconciliation to eventually fail becuse of the unrestartable nodes
        var ex = assertThrows(UnrestartableNodesException.class, () -> rr.loop(),
                "Expect timeout because neither broker 3 nor 4 can be rolled while respecting the min ISR on topic A");
        assertEquals("Cannot restart nodes {4,3} without violating some topics' min.in.sync.replicas", ex.getMessage());

        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(3)));
        Mockito.verify(platformClient, never()).restartNode(eq(nodeRefs.get(4)));

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void oddSizedQuorumOneControllerBehind() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 7000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // We should be able to roll only the controller that is behind
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void evenSizedQuorumTwoControllersBehind() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 7000L);
        quorumState.put(3, 6000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2, 4)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2, 4)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2, 4)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2, 4)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // we should not roll any controllers as the majority have not caught up to the leader
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollWithCustomControllerFetchTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 7000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(new ConfigEntry("controller.quorum.fetch.timeout.ms", "4000")), Set.of(), 0, 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        // We should be able to restart all the nodes because controller.quorum.fetch.timeout.ms value was increased
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 0);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        //active controller gets restarted last
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());

        for (var nodeRef : nodeRefs.values()) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
        }
    }

    @Test
    public void shouldNotRollControllersWithInvalidTimestamp() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, -1L);
        quorumState.put(1, 10000L);
        quorumState.put(2, -1L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldNotRollControllersWithInvalidLeader() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(0, 10000L);
        quorumState.put(1, 10000L);
        quorumState.put(2, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 0, 1, 2)
                .mockLeader(rollClient, -1)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollTwoNodesQuorumControllers() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(1, 10000L);
        quorumState.put(2, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollTwoNodesQuorumOneControllerBehind() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(1, 10000L);
        quorumState.put(2, 7000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 1, 2)
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);

        //only the controller that has fallen behind should be restarted
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }

    @Test
    public void shouldRollSingleNodeQuorum() throws ExecutionException, InterruptedException, TimeoutException {
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        Map<Integer, Long> quorumState = new HashMap<>();
        quorumState.put(1, 10000L);
        var nodeRefs = new MockBuilder()
                .addNodes(true, false, 1)
                .addNode(false, true, 2 )
                .mockLeader(rollClient, 1)
                .mockHealthyNodes(platformClient, rollClient, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 1, 2)
                .mockDescribeQuorum(rollClient, quorumState)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 1, 2)
                .done();

        var rr = RackRolling.rollingRestart(time,
                platformClient,
                rollClient,
                nodeRefs.values(),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                1);
        // TODO: the assertion fails with a single node cluster which is the reason why a broker node is added
        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 1);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr, 2);

        assertBrokerRestarted(platformClient, rollClient, nodeRefs, rr);

        assertEquals(List.of(), rr.loop());
    }
}
