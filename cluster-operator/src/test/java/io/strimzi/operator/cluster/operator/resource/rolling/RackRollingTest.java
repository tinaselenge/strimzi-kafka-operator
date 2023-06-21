package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.RestartReason;
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

        //Assuming the pods is Serving
        doReturn(State.SERVING).when(client).observe(brokerId);
        RackRolling.rollingRestart(client, asList(brokerId), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Set.of();
            }
        }, 1000, 1000, 1, 1);

    }

    @Test
    public void shouldRestartBrokerIfReasonManualRolling() throws ExecutionException, InterruptedException, TimeoutException {
        var brokerId = 0;
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(brokerId)).thenReturn(false);

        //Assuming the pods is Serving
        doReturn(State.SERVING).when(client).observe(brokerId);
        RackRolling.rollingRestart(client, asList(brokerId), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Set.of(RestartReason.MANUAL_ROLLING_UPDATE);
            }
        }, 1000, 1000, 1, 1);

    }
}
