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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

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

    private static RestartReasons configChange(int serverId) {
        return RestartReasons.of(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
    }

    private static List<NodeRef> listOfMultipleBrokerNodes() {
        return List.of(
                new NodeRef("pool-kafka-0", 0, "pool", false, false),
                new NodeRef("pool-kafka-1", 1, "pool", false, false),
                new NodeRef("pool-kafka-2", 2, "pool", false, false));
    }

    public void doRollingRestart(RollClient client,
                                 List<NodeRef> nodeRefList,
                                 Function<Integer, RestartReasons> reason,
                                 Function<Integer, String> kafkaConfigProvider,
                                 Integer maxRestartsBatchSize,
                                 Integer maxRestarts) throws ExecutionException, InterruptedException, TimeoutException {

        RackRolling.rollingRestart(time,
                client,
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
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(client, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(client, never()).deletePod(any());
        Mockito.verify(client, never()).tryElectAllPreferredLeaders(any());
    }

    private static void mockHealthyBroker(RollClient client, NodeRef nodeRef) {
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
    }

    private static void mockUnHealthyBroker(RollClient client, NodeRef nodeRef) {
        doReturn(true, false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.NOT_RUNNING, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
    }

    private final Set<TopicListing> topicListing = new HashSet<>();
    private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();

    @BeforeEach
    public void before() {
        topicListing.clear();
        topicDescriptions.clear();
    }

    private void addTopic(String topicName, Node leader) {
        addTopic(topicName, leader, List.of(leader), List.of(leader));
    }
    private void addTopic(String topicName, Node leader, List<Node> replicas, List<Node> isr) {
        Uuid topicId = Uuid.randomUuid();
        topicListing.add(new TopicListing(topicName, topicId, false));
        topicDescriptions.put(topicId, new TopicDescription(topicName, false,
                List.of(new TopicPartitionInfo(0,
                        leader, replicas, isr))));
    }
    
    private void mockTopics(RollClient client) throws ExecutionException, InterruptedException {
        doReturn(topicListing)
                .when(client)
                .listTopics();
        doAnswer(i -> {
            List<Uuid> topicIds = i.getArgument(0);
            return topicIds.stream().map(tid -> topicDescriptions.get(tid)).toList();
        })
                .when(client)
                .describeTopics(any());
    }

    @Test
    void shouldRestartBrokerWithNoTopicIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, nodeRef);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(client, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(client, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowMaxRestartsExceededIfBrokerRestartsMoreThanMaxRestarts() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        mockUnHealthyBroker(client, nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);

        // when
        var ex = assertThrows(MaxRestartsExceededException.class,
                () -> doRollingRestart(client, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 1));

        //then
        assertEquals("Broker 0 has been restarted 1 times", ex.getMessage());
    }

    @Test
    void shouldThrowTimeoutExceptionIfAllPreferredLeaderNotElected() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        mockUnHealthyBroker(client, nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(2)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);

        var te = assertThrows(TimeoutException.class,
              () -> doRollingRestart(client, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2));

        assertEquals("Failed to reach LEADING_ALL_PREFERRED within 117000: " +
                        "Context[nodeRef=pool-kafka-0/0, state=SERVING, " +
                        "lastTransition=1970-01-01T00:00:03Z, reason=[POD_UNRESPONSIVE], numRestarts=1]",
                te.getMessage());

    }


    @Test
    void shouldThrowTimeoutExceptionIfAllPreferredLeadersNotElected() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of(
                new ConfigEntry("compression.type", "zstd")
        )), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(1)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);
        doReturn(State.SERVING)
                .when(client)
                .observe(nodeRef);

        // when
        var te = assertThrows(TimeoutException.class,
                () -> doRollingRestart(client, List.of(nodeRef), RackRollingTest::configChange, serverId -> "compression.type=snappy", 1, 1));

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
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(1, 1, 1, 1, 0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);

        doRollingRestart(client, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2);

        Mockito.verify(client, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(5)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldThrowTimeoutExceptionIfPodNotAbleToRecoverAfterRestart() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING, BrokerState.NOT_RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);

        var te = assertThrows(TimeoutException.class,
                () -> doRollingRestart(client, List.of(nodeRef), RackRollingTest::podUnresponsive, EMPTY_CONFIG_SUPPLIER, 1, 2));

        assertEquals("Failed to reach SERVING within 120000 ms: " +
                        "Context[nodeRef=pool-kafka-0/0, state=RECOVERING, " +
                        "lastTransition=1970-01-01T00:00:01Z, reason=[POD_UNRESPONSIVE], numRestarts=1]",
                te.getMessage());
    }

    @Test
    void shouldRestartNotReadyBrokerEvenIfNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(true, false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 1, 1);

        // then
        Mockito.verify(client, never()).reconfigureNode(any(), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of(
                new ConfigEntry("compression.type", "zstd")
        )), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);
        doReturn(State.SERVING)
                .when(client)
                .observe(nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::configChange, serverId -> "compression.type=snappy", 1, 1);

        // then
        Mockito.verify(client, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(client, never()).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));

    }

    @Test
    void shouldRestartBrokerIfChangedNonReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of(
                new ConfigEntry("auto.leader.rebalance.enable", "true")
        )), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);
        doReturn(State.SERVING)
                .when(client)
                .observe(nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::configChange, serverId -> "auto.leader.rebalance.enable=false", 1, 1);

        // then
        Mockito.verify(client, never()).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldReconfigureBrokerIfChangedReconfigurableLoggingParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(false)
                .when(client)
                .isNotReady(nodeRef);
        doReturn(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(nodeRef);
        doCallRealMethod()
                .when(client)
                .observe(nodeRef);
        addTopic("topic-A", node);
        mockTopics(client);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of(
        )), new Config(Set.of(new ConfigEntry("log.retention.ms", "1000"))))))
                .when(client)
                .describeBrokerConfigs(List.of(nodeRef));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRef);
        doReturn(State.SERVING)
                .when(client)
                .observe(nodeRef);

        // when
        doRollingRestart(client, List.of(nodeRef), RackRollingTest::configChange, serverId -> "log.retention.ms=1000", 1, 1);

        // then
        Mockito.verify(client, times(1)).reconfigureNode(eq(nodeRef), any(), any());
        Mockito.verify(client, never()).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldNotRestartBrokersIfHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRefs = listOfMultipleBrokerNodes();
        List<Node> nodeList = new ArrayList<>();
        for (var nodeRef: nodeRefs) {
            nodeList.add(new Node(nodeRef.nodeId(), Node.noNode().host(), Node.noNode().port()));
        }
        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (var nodeRef: nodeRefs) {
            configPair.put(nodeRef.nodeId(), new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        RollClient client = mock(RollClient.class);
        for (var nodeRef: nodeRefs) {
            mockHealthyBroker(client, nodeRef);
        }
        addTopic("topic-0", nodeList.get(0));
        addTopic("topic-1", nodeList.get(1));
        addTopic("topic-2", nodeList.get(2));
        mockTopics(client);
        doReturn(configPair)
                .when(client)
                .describeBrokerConfigs(any());
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRefs.get(0));

        // when
        doRollingRestart(client, nodeRefs, RackRollingTest::noReasons, EMPTY_CONFIG_SUPPLIER, 3, 5);

        // then
        for (var nodeRef: nodeRefs) {
            Mockito.verify(client, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(client, never()).deletePod(any());
            Mockito.verify(client, never()).tryElectAllPreferredLeaders(any());
        }
    }

    @Test
    void shouldRestartBrokersIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRefs = listOfMultipleBrokerNodes();
        List<Node> nodeList = new ArrayList<>();
        for (var nodeRef: nodeRefs) {
            nodeList.add(new Node(nodeRef.nodeId(), Node.noNode().host(), Node.noNode().port()));
        }
        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (var nodeRef: nodeRefs) {
            configPair.put(nodeRef.nodeId(), new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        RollClient client = mock(RollClient.class);
        for (var nodeRef: nodeRefs) {
            mockHealthyBroker(client, nodeRef);
        }
        addTopic("topic-0", nodeList.get(0));
        addTopic("topic-1", nodeList.get(1));
        addTopic("topic-2", nodeList.get(2));
        mockTopics(client);
        doReturn(configPair)
                .when(client)
                .describeBrokerConfigs(any());
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(nodeRefs.get(0));

        // when
        doRollingRestart(client, nodeRefs, RackRollingTest::manualRolling, EMPTY_CONFIG_SUPPLIER, 3, 1);

        // then
        for (var nodeRef: nodeRefs) {
            Mockito.verify(client, never()).reconfigureNode(eq(nodeRef), any(), any());
            Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
            Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }

    // TODO assertions that the active controller is last
    // TODO assertions that controllers are always in different batches
    // TODO Tests for combined-mode brokers
    // TODO Tests for pure controllers
    // TODO Tests for pure brokers
    // TODO Tests for nodes with both pure-controllers and and pure brokers

    // TODO handling of exceptions from the admin client

}
