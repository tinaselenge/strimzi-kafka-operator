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

import java.util.ArrayList;
import java.util.Collection;
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

    public static final Function<Integer, String> EMPTY_CONFIG_SUPPLIER = serverId -> "";

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
    public void shouldNotRestartBrokersWhenAllHealthyAndNoReasons() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var brokerId = new NodeRef("pool-kafka-0", 0, "pool", false, false);

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, brokerId);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
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

    // TODO: Currently this test fails since no partitions are present on the broker(one of the edge case),
    //  We should make this test working
    @Test
    public void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = new NodeRef("pool-kafka-0", 0, "pool", false, false);

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, brokerId);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(brokerId));

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
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
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(any());
    }


    // TODO: Make the test when "no partitions on one broker" case

    @Test
    public void shouldRestartBrokerWithTopicWithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Uuid topicAId = Uuid.randomUuid();
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        mockHealthyBroker(client, brokerId);
        doReturn(Set.of(new TopicListing("topic-A", topicAId, true)))
                .when(client)
                .listTopics();
        doReturn(List.of(new TopicDescription("topic-A", false,
                List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))))))
                .when(client)
                .describeTopics(List.of(topicAId));
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(brokerId));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(brokerId);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
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
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(any());
    }

    @Test
    public void shouldRestartNotReadyBrokerWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Uuid topicAId = Uuid.randomUuid();
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(true, false)
                .when(client)
                .isNotReady(brokerId);
        doReturn(BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(brokerId);
        doCallRealMethod()
                .when(client)
                .observe(brokerId);
        doReturn(Set.of(new TopicListing("topic-A", topicAId, true)))
                .when(client)
                .listTopics();
        doReturn(List.of(new TopicDescription("topic-A", true,
                List.of(new TopicPartitionInfo(0,
                        node,
                        List.of(node), List.of(node))))))
                .when(client)
                .describeTopics(List.of(topicAId));
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(brokerId));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(brokerId);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
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
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(any());
    }

    @Test
    public void shouldReconfigureBrokerWithChangedReconfigurableParameter() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = new NodeRef("pool-kafka-0", 0, "pool", false, false);
        Uuid topicAId = Uuid.randomUuid();
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());

        RollClient client = mock(RollClient.class);
        doReturn(false)
                .when(client)
                .isNotReady(brokerId);
        doReturn(BrokerState.RUNNING, BrokerState.NOT_RUNNING, BrokerState.STARTING, BrokerState.RECOVERY, BrokerState.RUNNING)
                .when(client)
                .getBrokerState(brokerId);
        doCallRealMethod()
                .when(client)
                .observe(brokerId);
        doReturn(Set.of(new TopicListing("topic-A", topicAId, true)))
                .when(client)
                .listTopics();
        doReturn(List.of(new TopicDescription("topic-A", true,
                List.of(new TopicPartitionInfo(0,
                        node,
                        List.of(node), List.of(node))))))
                .when(client)
                .describeTopics(List.of(topicAId));
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of(
                new ConfigEntry("compression.type", "zstd")
        )), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(brokerId));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(brokerId);
        doReturn(State.SERVING)
                .when(client)
                .observe(brokerId);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
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
        Mockito.verify(client, times(1)).reconfigureServer(eq(brokerId),any(), any());
        // TODO Mockito.verify(client, never()).deletePod(eq(brokerId)); // need to convert between broker ids and pod names
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(brokerId));

    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerIds = List.of(
                new NodeRef("pool-kafka-0", 0, "pool", false, false),
                new NodeRef("pool-kafka-1", 1, "pool", false, false),
                new NodeRef("pool-kafka-2", 2, "pool", false, false));
        List<Node> nodeList = new ArrayList<>();
        for (var brokerId: brokerIds) {
            nodeList.add(new Node(brokerId.nodeId(), Node.noNode().host(), Node.noNode().port()));
        }
        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (var brokerId: brokerIds) {
            configPair.put(brokerId.nodeId(), new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }
        Collection<TopicListing> topicListings = new HashSet<>();
        for (var brokerId: brokerIds) {
            Uuid topicId = Uuid.randomUuid();
            topicListings.add(new TopicListing("topic-" + brokerId.nodeId(), topicId, true));
        }

        RollClient client = mock(RollClient.class);
        for (var brokerId: brokerIds) {
            mockHealthyBroker(client, brokerId);
        }
        doReturn(topicListings)
                .when(client)
                .listTopics();
        doAnswer(i -> List.of(new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(0,
                        nodeList.get(0),
                        List.of(nodeList.get(0)), List.of(nodeList.get(0))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(1,
                        nodeList.get(0),
                        List.of(nodeList.get(1)), List.of(nodeList.get(1))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(2,
                        nodeList.get(0),
                        List.of(nodeList.get(2)), List.of(nodeList.get(2)))))))
                .when(client)
                .describeTopics(topicListings.stream().map(TopicListing::topicId).toList());
        doReturn(configPair)
                .when(client)
                .describeBrokerConfigs(any());
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(brokerIds.get(0));

        // when
        RackRolling.rollingRestart(time,
                client,
                brokerIds,
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
        // TODO add assertions
    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerIds = List.of(
                new NodeRef("pool-kafka-0", 0, "pool", false, false),
                new NodeRef("pool-kafka-1", 1, "pool", false, false),
                new NodeRef("pool-kafka-2", 2, "pool", false, false));
        List<Node> nodeList = new ArrayList<>();
        for (var brokerId: brokerIds) {
            nodeList.add(new Node(brokerId.nodeId(), Node.noNode().host(), Node.noNode().port()));
        }
        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (var brokerId: brokerIds) {
            configPair.put(brokerId.nodeId(), new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }
        Collection<TopicListing> topicListings = new HashSet<>();
        for (var brokerId: brokerIds) {
            Uuid id = Uuid.randomUuid();
            topicListings.add(new TopicListing("topic-" + brokerId.nodeId(), id, true));
        }

        RollClient client = mock(RollClient.class);
        for (var brokerId: brokerIds) {
            mockHealthyBroker(client, brokerId);
        }
        doReturn(topicListings)
                .when(client)
                .listTopics();
        doAnswer(i -> List.of(new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(0,
                        nodeList.get(0),
                        List.of(nodeList.get(0)), List.of(nodeList.get(0))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(1,
                        nodeList.get(0),
                        List.of(nodeList.get(1)), List.of(nodeList.get(1))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(2,
                        nodeList.get(0),
                        List.of(nodeList.get(2)), List.of(nodeList.get(2)))))))
                .when(client)
                .describeTopics(topicListings.stream().map(TopicListing::topicId).toList());
        doReturn(configPair)
                .when(client)
                .describeBrokerConfigs(any());
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(brokerIds.get(0));

        // when
        RackRolling.rollingRestart(time,
                client,
                brokerIds,
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
        for (var brokerId: brokerIds) {
            Mockito.verify(client, never()).reconfigureServer(eq(brokerId), any(), any());
            // TODO Mockito.verify(client, times(1)).deletePod(eq(brokerId)); need to map between server id and pod name
            Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(eq(brokerId));
        }
    }

}
