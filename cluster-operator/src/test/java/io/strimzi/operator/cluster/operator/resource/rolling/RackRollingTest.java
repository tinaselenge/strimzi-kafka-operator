package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Config;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

public class RackRollingTest {

    @Test
    public void shouldRestartNoBrokersIfNoReason() throws ExecutionException, InterruptedException, TimeoutException {
        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        doReturn(State.SERVING).when(client).observe(brokerId);
        RackRolling.rollingRestart(client, asList(brokerId), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Set.of();
            }
        }, Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(), new Function<Integer, String>() {
            @Override
            public String apply(Integer integer) {
                return "";
            }
        }, null, 1000, 1000, 1, 1);

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
        RackRolling.rollingRestart(client, asList(brokerId), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Set.of(RestartReason.MANUAL_ROLLING_UPDATE);
            }
        }, Reconciliation.DUMMY_RECONCILIATION, KafkaVersionTestUtils.getLatestVersion(), new Function<Integer, String>() {
            @Override
            public String apply(Integer integer) {
                return "";
            }
        }, null, 1000, 1000, 1, 1);

    }

    // TODO: Make the test when "no partitions on one broker" case




}
