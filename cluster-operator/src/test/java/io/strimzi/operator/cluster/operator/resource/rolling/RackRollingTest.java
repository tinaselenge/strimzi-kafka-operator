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


    // TODO write tests with realistic clusters:
    // 3 Pure controllers, 3 brokers

    // 3 combined nodes
    // combinedModeCluster(3).node(1).healthy().node(2).unhealthy().activeController().node(3)...done()
    // Single node

    // TODO assertions that the active controller is last
    // TODO assertions that controllers are always in different batches
    // TODO Tests for combined-mode brokers
    // TODO Tests for pure controllers
    // TODO Tests for pure brokers
    // TODO Tests for nodes with both pure-controllers and and pure brokers

    // TODO handling of exceptions from the admin client


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

    private static RestartReasons configChange(int serverId) {
        return RestartReasons.of(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
    }

    private static List<NodeRef> listOfNodeRefs(int startNodeId, int num, boolean controller, boolean broker) {
        return IntStream.range(startNodeId, num).boxed().map(nodeId ->
                new NodeRef("pool-kafka-" + nodeId, nodeId, "pool", controller, broker)).toList();
    }

    static class MockBuilder {
        private final Map<Integer, NodeRef> nodeRefs = new HashMap<>();
        private final Map<Integer, Node> nodes = new HashMap<>();
        private final Set<TopicListing> topicListing = new HashSet<>();
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
            for (int nodeId: nodeIds) {
                addNode(controller, broker ,nodeId);
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
            for (var nodeId: nodeIds) {
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
            return addTopic(topicName, leaderId, List.of(leaderId), List.of(leaderId));
        }
        MockBuilder addTopic(String topicName, int leaderId, List<Integer> replicaIds, List<Integer> isrIds) {
            Uuid topicId = Uuid.randomUuid();
            topicListing.add(new TopicListing(topicName, topicId, false));
            Node leaderNode = nodes.get(leaderId);
            List<Node> replicas = replicaIds.stream().map(nodes::get).toList();
            List<Node> isr = isrIds.stream().map(nodes::get).toList();
            topicDescriptions.put(topicId, new TopicDescription(topicName, false,
                    List.of(new TopicPartitionInfo(0,
                            leaderNode, replicas, isr))));
            return this;
        }

        MockBuilder mockTopics(RollClient client) {
            doReturn(topicListing)
                    .when(client)
                    .listTopics();
            doAnswer(i -> {
                List<Uuid> topicIds = i.getArgument(0);
                return topicIds.stream().map(tid -> topicDescriptions.get(tid)).toList();
            })
                    .when(client)
                    .describeTopics(any());
            return this;
        }

        MockBuilder mockDescribeConfigs(RollClient rollClient, Set<ConfigEntry> nodeConfigs, Set<ConfigEntry> loggerConfigs, int... nodeIds) {
            Map<Integer, RollClient.Configs> configPair = new HashMap<>();
            for (var nodeId: nodeIds) {
                configPair.put(nodeId, new RollClient.Configs(new Config(nodeConfigs), new Config(loggerConfigs)));
            }
            doReturn(configPair)
                    .when(rollClient)
                    .describeBrokerConfigs(IntStream.of(nodeIds).boxed().map(nodeRefs::get).toList());
            return this;
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int nodeId) {
            return mockElectLeaders(rollClient, List.of(0), nodeId);
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, int... nodeIds) {
            return mockElectLeaders(rollClient, List.of(0), nodeIds);
        }

        public MockBuilder mockElectLeaders(RollClient rollClient, List<Integer> results, int... nodeIds) {
            for (var nodeId: nodeIds) {
                doReturn(results.get(0), results.subList(1, results.size()).toArray())
                        .when(rollClient)
                        .tryElectAllPreferredLeaders(nodeRefs.get(nodeId));
            }
            return this;
        }

        public List<NodeRef> done() {
            return List.copyOf(nodeRefs.values());
        }
    }


    private void doRollingRestart(PlatformClient platformClient,
                                 RollClient rollClient,
                                 List<NodeRef> nodeRefList,
                                 Function<Integer, RestartReasons> reason,
                                 Function<Integer, String> kafkaConfigProvider,
                                 Integer maxRestartsBatchSize,
                                 Integer maxRestarts) throws ExecutionException, InterruptedException, TimeoutException {

        RackRolling.rollingRestart(time,
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
    }

    @Test
    void shouldNotRestartBrokersWithNoTopicsIfAllHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        PlatformClient platformClient = mock(PlatformClient.class);
        RollClient rollClient = mock(RollClient.class);
        var nodeRef = new MockBuilder()
                .addNode(false, true, 0)
                .mockHealthyNode(platformClient, rollClient, 0)
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
                () -> doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::configChange, serverId -> "compression.type=snappy", 1, 1));

        // then
        assertEquals("Failed to reach LEADING_ALL_PREFERRED within 15000: " +
                        "Context[nodeRef=pool-kafka-0/0, state=RECONFIGURED, " +
                        "lastTransition=1970-01-01T00:00:00Z, " +
                        "reason=[CONFIG_CHANGE_REQUIRES_RESTART], numRestarts=0]",
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
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::configChange, serverId -> "compression.type=snappy", 1, 1);

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
                                Set.of(
                new ConfigEntry("auto.leader.rebalance.enable", "true")),
Set.of(), 0)
                                .mockElectLeaders(rollClient, 0)
                                        .done().get(0);

        // when
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::configChange, serverId -> "auto.leader.rebalance.enable=false", 1, 1);

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
        doRollingRestart(platformClient, rollClient, List.of(nodeRef), RackRollingTest::configChange, serverId -> "log.retention.ms=1000", 1, 1);

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
        doRollingRestart(platformClient, rollClient, nodeRefs, RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 3, 5);

        // then
        for (var nodeRef: nodeRefs) {
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
                .addTopic("topic-2", 1)
                .addTopic("topic-2", 2)
                .mockHealthyNodes(platformClient, rollClient, 0, 1, 2)
                .mockDescribeConfigs(rollClient, Set.of(), Set.of(), 0, 1, 2)
                .mockTopics(rollClient)
                .mockElectLeaders(rollClient, 0, 1, 2)
                .done();

        // when
        doRollingRestart(platformClient, rollClient, nodeRefs,
                RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3, 1);

        // then
        for (var nodeRef: nodeRefs) {
            Mockito.verify(rollClient, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(platformClient, times(1)).restartNode(eq(nodeRef));
            Mockito.verify(rollClient, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }

}
