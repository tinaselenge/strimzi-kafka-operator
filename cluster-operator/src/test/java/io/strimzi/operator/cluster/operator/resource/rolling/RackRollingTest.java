/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.Test;
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
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class RackRollingTest {

    public static final Function<Integer, String> EMPTY_CONFIG_SUPPLIER = serverId -> "";

    private final Time time = new Time.TestTime();

    static Set<RestartReason> noReasons(int serverId) {
        return Set.of();
    }

    private static Set<RestartReason> manualRolling(int serverId) {
        return Set.of(RestartReason.MANUAL_ROLLING_UPDATE);
    }

    @Test
    public void shouldNotRestartBrokersWhenAllHealthyAndNoReasons() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var brokerId = 0;
        RollClient client = mock(RollClient.class);
        when(client.isNotReady(brokerId)).thenReturn(false);
        doReturn(State.SERVING).when(client).observe(brokerId);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(anyInt());
        Mockito.verify(client, never()).deletePod(any());
        Mockito.verify(client, never()).tryElectAllPreferredLeaders(anyInt());
    }

    // TODO: Currently this test fails since no partitions are present on the broker(one of the edge case),
    //  We should make this test working
    @Test
    public void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        //Assuming the pods is Serving
        doReturn(State.SERVING).when(client).observe(brokerId);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())))).when(client).describeBrokerConfigs(List.of(0));

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(anyInt());
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(anyInt());
    }


    // TODO: Make the test when "no partitions on one broker" case

    @Test
    public void shouldRestartBrokerWithTopicWithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = 0;
        RollClient client = mock(RollClient.class);
        when(client.isNotReady(brokerId)).thenReturn(false);
        doReturn(State.SERVING).when(client).observe(brokerId);
        Uuid id = Uuid.randomUuid();

        doReturn(Set.of(new TopicListing("topic-A", id, true))).when(client).listTopics();
        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());
        doReturn(Stream.of(new TopicDescription("topic-A", true,
        List.of(new TopicPartitionInfo(0,
                node,
        List.of(node), List.of(node)))))).when(client).describeTopics(List.of(id));

        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())))).when(client).describeBrokerConfigs(List.of(0));
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                1,
                1);

        // then
        Mockito.verify(client, never()).reconfigureServer(anyInt());
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(anyInt());
    }

    @Test
    public void shouldRestartNotReadyBrokerWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        doReturn(true, false)
                .when(client)
                .isNotReady(brokerId);
        doReturn(1, 2, 3)
                .when(client)
                .getBrokerState(brokerId);
        doCallRealMethod().when(client)
                .observe(0);

        Uuid id = Uuid.randomUuid();
        doReturn(Set.of(new TopicListing("topic-A", id, true)))
                .when(client)
                .listTopics();

        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());
        doReturn(Stream.of(new TopicDescription("topic-A", true,
                List.of(new TopicPartitionInfo(0,
                        node,
                        List.of(node), List.of(node))))))
                .when(client)
                .describeTopics(List.of(id));


        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of()))))
                .when(client)
                .describeBrokerConfigs(List.of(0));
        doReturn(0)
                .when(client)
                .tryElectAllPreferredLeaders(0);

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
        Mockito.verify(client, never()).reconfigureServer(anyInt());
        Mockito.verify(client, times(1)).deletePod(any());
        Mockito.verify(client, times(1)).tryElectAllPreferredLeaders(anyInt());
    }

    @Test
    public void shouldRestartReconfiguredBrokerWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerId = 0;
        RollClient client = mock(RollClient.class);
        when(client.isNotReady(brokerId)).thenReturn(false);
        doReturn(State.RECONFIGURED).when(client).observe(brokerId);
        Uuid id = Uuid.randomUuid();
        doReturn(Set.of(new TopicListing("topic-A", id, true))).when(client).listTopics();

        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());
        doReturn(Stream.of(new TopicDescription("topic-A", true,
                List.of(new TopicPartitionInfo(0,
                        node,
                        List.of(node), List.of(node)))))).when(client).describeTopics(List.of(id));

        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())))).when(client).describeBrokerConfigs(List.of(0));
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);
        doReturn(State.SERVING).when(client).observe(0);

        // when
        RackRolling.rollingRestart(time,
                client,
                List.of(brokerId),
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                1,
                1);

        // then
        // TODO add assertions

    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerIds = List.of(0, 1, 2);
        RollClient client = mock(RollClient.class);

        for (Integer brokerId: brokerIds) {
            when(client.isNotReady(brokerId)).thenReturn(false);
        }

        doReturn(State.SERVING).when(client).observe(0);
        doReturn(State.SERVING).when(client).observe(1);
        doReturn(State.SERVING).when(client).observe(2);

        Collection<TopicListing> topicListings = new HashSet<>();
        for (Integer brokerId: brokerIds) {
            Uuid id = Uuid.randomUuid();
            topicListings.add(new TopicListing("topic-" + brokerId, id, true));
        }

        doReturn(topicListings).when(client).listTopics();

        List<Node> nodeList = new ArrayList<>();
        for (Integer brokerId: brokerIds) {
            nodeList.add(new Node(brokerId, Node.noNode().host(), Node.noNode().port()));
        }

        when(client.describeTopics(topicListings.stream().map(TopicListing::topicId).toList())).thenAnswer(i -> Stream.of(new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(0,
                        nodeList.get(0),
                        List.of(nodeList.get(0)), List.of(nodeList.get(0))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(1,
                        nodeList.get(0),
                        List.of(nodeList.get(1)), List.of(nodeList.get(1))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(2,
                        nodeList.get(0),
                        List.of(nodeList.get(2)), List.of(nodeList.get(2)))))));

        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (Integer brokerId: brokerIds) {
            configPair.put(brokerId, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        doReturn(configPair).when(client).describeBrokerConfigs(any());
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);

        // when
        RackRolling.rollingRestart(time,
                client,
                brokerIds,
                RackRollingTest::noReasons,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                3,
                5);

        // then
        // TODO add assertions
    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        // given
        var brokerIds = List.of(0, 1, 2);
        RollClient client = mock(RollClient.class);

        for (Integer brokerId: brokerIds) {
            when(client.isNotReady(brokerId)).thenReturn(false);
        }

        doReturn(State.SERVING).when(client).observe(0);
        doReturn(State.SERVING).when(client).observe(1);
        doReturn(State.SERVING).when(client).observe(2);

        Collection<TopicListing> topicListings = new HashSet<>();
        for (Integer brokerId: brokerIds) {
            Uuid id = Uuid.randomUuid();
            topicListings.add(new TopicListing("topic-" + brokerId, id, true));
        }

        doReturn(topicListings).when(client).listTopics();

        List<Node> nodeList = new ArrayList<>();
        for (Integer brokerId: brokerIds) {
            nodeList.add(new Node(brokerId, Node.noNode().host(), Node.noNode().port()));
        }

        when(client.describeTopics(topicListings.stream().map(TopicListing::topicId).toList())).thenAnswer(i -> Stream.of(new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(0,
                        nodeList.get(0),
                        List.of(nodeList.get(0)), List.of(nodeList.get(0))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(1,
                        nodeList.get(0),
                        List.of(nodeList.get(1)), List.of(nodeList.get(1))))), new TopicDescription("topic-1", true,
                List.of(new TopicPartitionInfo(2,
                        nodeList.get(0),
                        List.of(nodeList.get(2)), List.of(nodeList.get(2)))))));

        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (Integer brokerId: brokerIds) {
            configPair.put(brokerId, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        doReturn(configPair).when(client).describeBrokerConfigs(any());
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);

        // when
        RackRolling.rollingRestart(time,
                client,
                brokerIds,
                RackRollingTest::manualRolling,
                Reconciliation.DUMMY_RECONCILIATION,
                KafkaVersionTestUtils.getLatestVersion(),
                EMPTY_CONFIG_SUPPLIER,
                null,
                1000,
                1000,
                3,
                5);

        // then
        // TODO add assertions
    }

}
