package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.RestartReason;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;

public class RackRollerTest {


    @Test
    public void dosuccessfulRolling() throws ExecutionException, InterruptedException, TimeoutException {
        RollClient client = mock(MockRollClient.class);
        RackRolling.rollingRestart(client, asList(0,1,2), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Collections.emptySet();
            }
        }, 10, 10, 2, 0);


    }

    static class MockRollClient implements RollClient {

        private final static int ADMIN_BATCH_SIZE = 200;

        @Override
        public boolean isNotReady(Integer nodeId) {
            return false;
        }

        @Override
        public State observe(int serverId) {
            return RollClient.super.observe(serverId);
        }

        // Assumed broker state is serving for all the pods
        @Override
        public int getBrokerState(Integer nodeId) {
            return 3;
        }

        @Override
        public void deletePod(String podName) {
        }

        @Override
        public Collection<TopicListing> listTopics() throws ExecutionException, InterruptedException {
            return Collections.emptyList();
        }

        @Override
        public Stream<TopicDescription> describeTopics(List<Uuid> topicIds) throws InterruptedException, ExecutionException {
            return Stream.empty();
        }

        @Override
        public Map<String, Integer> describeTopicMinIsrs(List<String> topicNames) throws InterruptedException, ExecutionException {
            return Map.of();
        }

        @Override
        public int activeController() throws InterruptedException, ExecutionException {
            return 1;
        }

        @Override
        public void reconfigureServer(int serverId) {
        }

        @Override
        public int tryElectAllPreferredLeaders(int serverId) {
            return 0;
        }
    }
}
