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
import org.junit.Before;
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

    private final Time time = new Time.TestTime();

    static RestartReasons noReasons(int serverId) {
        return RestartReasons.empty();
    }

    private static RestartReasons manualRolling(int serverId) {
        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
    }

    private static RestartReasons configChange(int serverId) {
        return RestartReasons.of(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
    }

    @Test
    void shouldNotRestartBrokersWithNoTopicsIfAllHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var nodeRef = new NodeRef("pool-kafka-0", 0, "pool", false, false);

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, nodeRef);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(any(), any(), any());
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

    private final Set<TopicListing> topicListing = new HashSet<>();
    private final Map<Uuid, TopicDescription> topicDescriptions = new HashMap<>();

    @Before
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(any(), any(), any());
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(any(), any(), any());
        Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(any(), any(), any());
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::configChange,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                serverId -> "compression.type=snappy",
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, times(1)).reconfigureServer(eq(nodeRef),any(), any());
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::configChange,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                serverId -> "auto.leader.rebalance.enable=false",
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(eq(nodeRef),any(), any());
        Mockito.verify(client,times(1)).deletePod(eq(nodeRef));
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
        RackRolling.rollingRestart(time,
                client,
                List.of(nodeRef),
                RackRollingTest::configChange,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                serverId -> "log.retention.ms=1000",
                null,
                30_000,
                120_000,
                1,
                1);

        // then
        Mockito.verify(client, times(1)).reconfigureServer(eq(nodeRef),any(), any());
        Mockito.verify(client, never()).deletePod(eq(nodeRef));
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
    }

    @Test
    void shouldNotRestartBrokersIfHealthyAndNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRefs = List.of(
                new NodeRef("pool-kafka-0", 0, "pool", false, false),
                new NodeRef("pool-kafka-1", 1, "pool", false, false),
                new NodeRef("pool-kafka-2", 2, "pool", false, false));
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
        RackRolling.rollingRestart(time,
                client,
                nodeRefs,
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                5);

        // then
        for (var nodeRef: nodeRefs) {
            Mockito.verify(client, never()).reconfigureServer(eq(nodeRef), any(), any());
            Mockito.verify(client, never()).deletePod(any());
            Mockito.verify(client, never()).tryElectAllPreferredLeaders(any());
        }
    }

    @Test
    void shouldRestartBrokersIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var nodeRefs = List.of(
                new NodeRef("pool-kafka-0", 0, "pool", false, false),
                new NodeRef("pool-kafka-1", 1, "pool", false, false),
                new NodeRef("pool-kafka-2", 2, "pool", false, false));
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
        RackRolling.rollingRestart(time,
                client,
                nodeRefs,
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                30_000,
                120_000,
                3,
                5);

        // then
        for (var nodeRef: nodeRefs) {
            Mockito.verify(client, never()).reconfigureServer(eq(nodeRef), any(), any());
            Mockito.verify(client, times(1)).deletePod(eq(nodeRef));
            Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(nodeRef));
        }
    }

    // TODO assertions that the active controller is last
    // TODO assertions that controllers are always in different batches
    // TODO test that exceeding maxRestart results in exception
    // TODO test that exceeding postReconfigureTimeoutMs results in exception
    // TODO test that exceeding postRestartTimeoutMs results in exception in all the possible cases:
    //    the broker state not becoming ready (and that we don't retry restarting in this case)
    //    tryElectAllPreferredLeaders not returning 0

    // TODO handling of exceptions from the admin client

}
