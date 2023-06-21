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
import static org.mockito.Mockito.*;

public class RackRollerTest {


    @Test
    public void dosuccessfulRolling() throws ExecutionException, InterruptedException, TimeoutException {
        RollClient client = mock(RollClient.class);

        when(client.isNotReady(0)).thenReturn(false);

        //Assuming the pods is Serving
        doReturn(3).when(client).getBrokerState(0);
        doReturn(State.SERVING).when(client).observe(0);
        doReturn(0).when(client).tryElectAllPreferredLeaders(0);
        doReturn(0).when(client).activeController();
        RackRolling.rollingRestart(client, asList(0), new Function<Integer, Set<RestartReason>>() {
            @Override
            public Set<RestartReason> apply(Integer integer) {
                return Collections.emptySet();
            }
        }, 1000, 1000, 1, 0);
    }
}
