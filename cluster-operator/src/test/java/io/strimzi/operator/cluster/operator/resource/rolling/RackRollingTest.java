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

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

public class RackRollingTest {

    public String EMPTY_CONFIG_SUPPLIER = "";

    @Test
    public void shouldRestartNoBrokersIfNoReason() throws ExecutionException, InterruptedException, TimeoutException {
        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        doReturn(State.SERVING).when(client).observe(brokerId);
        RackRolling.rollingRestart(client, asList(brokerId),
                integer -> Set.of(),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 1, 1);

    }

    // TODO: Currently this test fails since no partitions are present on the broker(one of the edge case),
    //  We should make this test working
    @Test
    public void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        //Assuming the pods is Serving
        doReturn(State.SERVING).when(client).observe(brokerId);
        doReturn(Map.of(0, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())))).when(client).describeBrokerConfigs(List.of(0));
        RackRolling.rollingRestart(client, asList(brokerId),
                integer -> Set.of(RestartReason.MANUAL_ROLLING_UPDATE),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 1, 1);

    }

    // TODO: Make the test when "no partitions on one broker" case

    @Test
    public void shouldRestartBrokerWithTopicwithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

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
        RackRolling.rollingRestart(client, List.of(brokerId),
                integer -> Set.of(RestartReason.MANUAL_ROLLING_UPDATE),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 1, 1);

    }

    @Test
    public void shouldRestartNotReadyBrokerWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        doReturn(State.NOT_READY).when(client).observe(brokerId);
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
        RackRolling.rollingRestart(client, asList(brokerId),
                integer -> Set.of(),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 1, 1);

    }

    @Test
    public void shouldRestartReconfiguredBrokerWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

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
        RackRolling.rollingRestart(client, List.of(brokerId),
                integer -> Set.of(),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 1, 1);

    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithNoReason() throws ExecutionException, InterruptedException, TimeoutException {

        var brokerIds = List.of(0, 1, 2);
        RollClient client = mock(RollClient.class);

        for (Integer brokerId: brokerIds){
            when(client.isNotReady(brokerId)).thenReturn(false);
        }

        doReturn(State.SERVING).when(client).observe(0);
        doReturn(State.SERVING).when(client).observe(1);
        doReturn(State.SERVING).when(client).observe(2);


        Collection<TopicListing> topicListings = new HashSet<>();
        for (Integer brokerId: brokerIds){
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
                        nodeList, nodeList)))));

        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (Integer brokerId: brokerIds) {
            configPair.put(brokerId, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        doReturn(configPair).when(client).describeBrokerConfigs(any());
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);

        RackRolling.rollingRestart(client, brokerIds, integer -> Set.of(),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 3, 5);

    }

    @Test
    public void shouldRestartMultipleBrokersWithTopicWithReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {

        var brokerIds = List.of(0, 1, 2);
        RollClient client = mock(RollClient.class);

        for (Integer brokerId: brokerIds){
            when(client.isNotReady(brokerId)).thenReturn(false);
        }

        doReturn(State.SERVING).when(client).observe(0);
        doReturn(State.SERVING).when(client).observe(1);
        doReturn(State.SERVING).when(client).observe(2);


        Collection<TopicListing> topicListings = new HashSet<>();
        for (Integer brokerId: brokerIds){
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
                        nodeList, nodeList)))));

        Map<Integer, RollClient.Configs> configPair = new HashMap<>();
        for (Integer brokerId: brokerIds) {
            configPair.put(brokerId, new RollClient.Configs(new Config(Set.of()), new Config(Set.of())));
        }

        doReturn(configPair).when(client).describeBrokerConfigs(any());
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);

        RackRolling.rollingRestart(client, brokerIds,
                integer -> Set.of(RestartReason.MANUAL_ROLLING_UPDATE),
                Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(),
                integer -> EMPTY_CONFIG_SUPPLIER,
                null, 1000, 1000, 3, 5);

    }

}
