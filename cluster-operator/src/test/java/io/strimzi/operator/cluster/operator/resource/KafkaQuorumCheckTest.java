package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class KafkaQuorumCheckTest {

    private Admin setUpMocks(int leaderId, Map<Integer, OptionalLong> replicaStateMap) {
        Admin admin = mock(Admin.class);
        DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
        when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
        QuorumInfo qrminfo = mock(QuorumInfo.class);
        List<QuorumInfo.ReplicaState> voters = replicaStateMap.entrySet().stream().map(entry -> {
            QuorumInfo.ReplicaState replicaState = mock(QuorumInfo.ReplicaState.class);
            when(replicaState.replicaId()).thenReturn(entry.getKey());
            when(replicaState.lastCaughtUpTimestamp()).thenReturn(entry.getValue());
            return replicaState;
        }).toList();
        when(qrminfo.voters()).thenReturn(voters);
        when(qrminfo.leaderId()).thenReturn(leaderId);
        when(qrmResult.quorumInfo()).thenReturn(KafkaFuture.completedFuture(qrminfo));
        return admin;
    }

    @Test
    public void canRollReturnsTrue(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(10000L));
        voters.put(2, OptionalLong.of(9500L));
        voters.put(3, OptionalLong.of(9700L));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));

    }

    @Test
    public void canRollReturnsTrueWith1FollowerBehind(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(10000L));
        voters.put(2, OptionalLong.of(7000L));
        voters.put(3, OptionalLong.of(9700L));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollReturnsFalse(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(10000L));
        voters.put(2, OptionalLong.of(7000L));
        voters.put(3, OptionalLong.of(9000L));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollReturnsFalseInvalidLeaderTimestamp(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(-1));
        voters.put(2, OptionalLong.of(7000L));
        voters.put(3, OptionalLong.of(9000L));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollReturnsFalseOneInvalidFollowerTimestamp(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(10000L));
        voters.put(2, OptionalLong.of(-1));
        voters.put(3, OptionalLong.of(9500L));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertTrue(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollReturnsFalseInvalidFollowersTimestamp(VertxTestContext context) {
        Map<Integer, OptionalLong> voters = new HashMap<>();
        voters.put(1, OptionalLong.of(10000L));
        voters.put(2, OptionalLong.of(-1));
        voters.put(3, OptionalLong.of(-1));
        Admin admin = setUpMocks(1, voters);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.succeeding(result -> {
            context.verify(() -> assertFalse(result));
            context.completeNow();
        }));
    }

    @Test
    public void canRollReturnsFailedFuture(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
        KafkaFutureImpl<QuorumInfo> kafkaFuture = new KafkaFutureImpl<>();
        String expectedError = "admin client not initialised";
        kafkaFuture.completeExceptionally(new Exception(expectedError));
        when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);
        when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
        KafkaQuorumCheck quorumCheck = new KafkaQuorumCheck(Reconciliation.DUMMY_RECONCILIATION, admin);
        quorumCheck.canRoll(1).onComplete(context.failing(error -> {
            context.verify(() -> assertThat(error.getMessage(), is(expectedError)));
            context.completeNow();
        }));
    }

}