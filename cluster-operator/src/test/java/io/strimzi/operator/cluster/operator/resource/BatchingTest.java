/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchingTest {

    private static final Reconciliation RECONCILIATION = Reconciliation.DUMMY_RECONCILIATION;

    /**
     * Helper to create a Node
     */
    private Node node(int id) {
        return new Node(id, "broker-" + id, 9092);
    }

    /**
     * Helper to create a TopicDescription with a single partition
     */
    private TopicDescription topic(String name, int leader, List<Integer> replicaIds, List<Integer> isrIds) {
        Node leaderNode = node(leader);
        List<Node> replicas = replicaIds.stream().map(this::node).toList();
        List<Node> isr = isrIds.stream().map(this::node).toList();
        TopicPartitionInfo partition = new TopicPartitionInfo(0, leaderNode, replicas, isr);
        return new TopicDescription(name, false, List.of(partition));
    }

    @Test
    void testEmptyNodeSet() {
        Batching batching = new Batching(RECONCILIATION, List.of());
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(), 10);
        assertThat(result, is(Set.of()));
    }

    @Test
    void testSingleNode() {
        TopicDescription topic1 = topic("topic1", 0, List.of(0, 1, 2), List.of(0, 1, 2));
        Batching batching = new Batching(RECONCILIATION, List.of(topic1));

        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0), 10);
        assertThat(result, is(Set.of(0)));
    }

    @Test
    void testNodesWithNoCommonReplicas() {
        // Topic A: brokers 0, 1, 2
        // Topic B: brokers 3, 4, 5
        // Nodes 0 and 3 have no common replicas, so they can be batched together
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1, 2), List.of(0, 1, 2));
        TopicDescription topicB = topic("topicB", 3, List.of(3, 4, 5), List.of(3, 4, 5));

        Batching batching = new Batching(RECONCILIATION, List.of(topicA, topicB));

        // All nodes have no common replicas with each other within their groups
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 3), 10);
        assertThat("Nodes with no common replicas should be batched together", result, is(Set.of(0, 3)));
    }

    @Test
    void testNodesWithCommonReplicas() {
        // Topic A has replicas on brokers 0, 1, 2
        // All three share replicas on the same topic, so they cannot be batched together
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1, 2), List.of(0, 1, 2));

        Batching batching = new Batching(RECONCILIATION, List.of(topicA));

        // When nodes share replicas, only one should be returned
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2), 10);
        assertThat("Should return only one node when all share replicas", result.size(), is(1));
        assertTrue(Set.of(0, 1, 2).contains(result.iterator().next()));
    }

    @Test
    void testMaxBatchSizeRespected() {
        // Create 6 brokers with no common replicas
        TopicDescription topic0 = topic("topic0", 0, List.of(0), List.of(0));
        TopicDescription topic1 = topic("topic1", 1, List.of(1), List.of(1));
        TopicDescription topic2 = topic("topic2", 2, List.of(2), List.of(2));
        TopicDescription topic3 = topic("topic3", 3, List.of(3), List.of(3));
        TopicDescription topic4 = topic("topic4", 4, List.of(4), List.of(4));
        TopicDescription topic5 = topic("topic5", 5, List.of(5), List.of(5));

        Batching batching = new Batching(RECONCILIATION,
            List.of(topic0, topic1, topic2, topic3, topic4, topic5));

        // Max batch size is 3, so we should get at most 3 nodes
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2, 3, 4, 5), 3);
        assertThat("Batch size should respect maxBatchSize", result.size(), is(3));
    }

    @Test
    void testMultipleCellsPicksLargest() {
        // Create two cells:
        // Cell 1: Nodes 0, 1 share topic A (they cannot be batched together)
        // Cell 2: Nodes 2, 3, 4 each have their own topics (they can be batched together)
        // The largest cell should be picked
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1), List.of(0, 1));
        TopicDescription topicB = topic("topicB", 2, List.of(2), List.of(2));
        TopicDescription topicC = topic("topicC", 3, List.of(3), List.of(3));
        TopicDescription topicD = topic("topicD", 4, List.of(4), List.of(4));

        Batching batching = new Batching(RECONCILIATION,
            List.of(topicA, topicB, topicC, topicD));

        // Nodes 2, 3, 4 have no replicas in common, forming the largest cell
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2, 3, 4), 10);

        // The result should be the largest batch - could be all of 2,3,4 together,
        // or just verify it's > 1 and doesn't include both 0 and 1 (which share a topic)
        assertEquals(3, result.size(), "Should pick a batch with 3 nodes");

        // Nodes 0 and 1 cannot both be in the batch since they share topicA
        if (result.contains(0) && result.contains(1)) {
            throw new AssertionError("Nodes 0 and 1 share a topic and cannot be batched together");
        }
    }

    @Test
    void testComplexTopology() {
        // Complex setup:
        // - Node 0 can only be batched alone (shares with 1, 2)
        // - Node 1 cannot batch with 0, 3, 4
        // - Node 3 can batch with 0 (no common topics)
        // - etc.
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1, 2), List.of(0, 1, 2));
        TopicDescription topicB = topic("topicB", 1, List.of(1, 3, 4), List.of(1, 3, 4));
        TopicDescription topicC = topic("topicC", 2, List.of(2, 4, 5), List.of(2, 4, 5));

        Batching batching = new Batching(RECONCILIATION,
            List.of(topicA, topicB, topicC));

        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2, 3, 4, 5), 10);

        // The result should be a valid batch (nodes with no common replicas)
        // Let's verify no two nodes in the result share a topic
        for (int node1 : result) {
            for (int node2 : result) {
                if (node1 != node2) {
                    // Verify they don't share any topic
                    boolean sharesTopic = false;
                    for (TopicDescription topic : List.of(topicA, topicB, topicC)) {
                        List<Integer> replicas = topic.partitions().get(0).replicas().stream()
                            .map(Node::id).toList();
                        if (replicas.contains(node1) && replicas.contains(node2)) {
                            sharesTopic = true;
                            break;
                        }
                    }
                    assertThat("Nodes " + node1 + " and " + node2 + " in batch share a topic",
                        sharesTopic, is(false));
                }
            }
        }
    }

    @Test
    void testRackAwareBatching() {
        // Simulating rack-aware topology:
        // Topic A: replicas on brokers 0 (rack X), 1 (rack Y), 2 (rack Z)
        // Topic B: replicas on brokers 3 (rack X), 4 (rack Y), 5 (rack Z)
        // Brokers in different racks with different topics can be batched
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1, 2), List.of(0, 1, 2));
        TopicDescription topicB = topic("topicB", 3, List.of(3, 4, 5), List.of(3, 4, 5));

        Batching batching = new Batching(RECONCILIATION, List.of(topicA, topicB));

        // Nodes from different topics can be batched together
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2, 3, 4, 5), 10);

        // Should return a batch of nodes with no common replicas
        // Could be {0,3}, {0,4}, {0,5}, {1,3}, {1,4}, {1,5}, {2,3}, {2,4}, or {2,5}
        assertEquals(2, result.size(), "Should pick a batch with 2 nodes");
        Set<Set<Integer>> possibleBatches = Set.of(Set.of(0, 3), Set.of(0, 4), Set.of(0, 5), Set.of(1, 3), Set.of(1, 4), Set.of(1, 5), Set.of(2, 3), Set.of(2, 4), Set.of(2, 5));
        assertTrue(possibleBatches.contains(result), "The batch should match one of the possible batches");
    }

    @Test
    void testSingleBatchSize() {
        // When maxBatchSize is 1, only one node should be returned
        TopicDescription topic0 = topic("topic0", 0, List.of(0), List.of(0));
        TopicDescription topic1 = topic("topic1", 1, List.of(1), List.of(1));
        TopicDescription topic2 = topic("topic2", 2, List.of(2), List.of(2));

        Batching batching = new Batching(RECONCILIATION, List.of(topic0, topic1, topic2));

        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2), 1);
        assertThat("With maxBatchSize=1, should return only one node", result.size(), is(1));
    }

    @Test
    void testMultiplePartitionsSameTopic() {
        // Topic A has two partitions with different replica sets
        // Partition 0: brokers 0, 1, 2
        // Partition 1: brokers 3, 4, 5
        Node leaderNode0 = node(0);
        Node leaderNode3 = node(3);
        List<Node> replicas0 = List.of(node(0), node(1), node(2));
        List<Node> replicas1 = List.of(node(3), node(4), node(5));

        TopicPartitionInfo partition0 = new TopicPartitionInfo(0, leaderNode0, replicas0, replicas0);
        TopicPartitionInfo partition1 = new TopicPartitionInfo(1, leaderNode3, replicas1, replicas1);
        TopicDescription topicA = new TopicDescription("topicA", false, List.of(partition0, partition1));

        Batching batching = new Batching(RECONCILIATION, List.of(topicA));

        // Nodes 0 and 3 are on different partitions but same topic
        // However, since they don't share the same partition, they can be batched
        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 3), 10);
        assertThat("Nodes on different partitions of same topic can batch", result, is(Set.of(0, 3)));
    }

    @Test
    void testNoTopics() {
        // When there are no topics, all nodes can be batched together
        Batching batching = new Batching(RECONCILIATION, List.of());

        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2), 10);
        assertThat("With no topics, all nodes can be batched", result, is(Set.of(0, 1, 2)));
    }

    @Test
    void testPartiallyOverlappingReplicas() {
        // Topic A: brokers 0, 1, 2
        // Topic B: brokers 1, 2, 3
        // Topic C: brokers 3, 4, 5
        //
        // Expected batching:
        // - 0 can batch with 3, 4, 5
        // - 1, 2, 3 share topics so can't all batch together
        TopicDescription topicA = topic("topicA", 0, List.of(0, 1, 2), List.of(0, 1, 2));
        TopicDescription topicB = topic("topicB", 1, List.of(1, 2, 3), List.of(1, 2, 3));
        TopicDescription topicC = topic("topicC", 3, List.of(3, 4, 5), List.of(3, 4, 5));

        Batching batching = new Batching(RECONCILIATION, List.of(topicA, topicB, topicC));

        Set<Integer> result = batching.getBatchedBrokersToRestart(Set.of(0, 1, 2, 3, 4, 5), 10);

        // Verify the result is valid (no two nodes share a partition)
        Collection<TopicDescription> topics = List.of(topicA, topicB, topicC);
        for (int node1 : result) {
            for (int node2 : result) {
                if (node1 != node2) {
                    for (TopicDescription topic : topics) {
                        List<Integer> replicas = topic.partitions().get(0).replicas().stream()
                            .map(Node::id).toList();
                        assertThat("Nodes " + node1 + " and " + node2 + " should not share partition on " + topic.name(),
                            replicas.contains(node1) && replicas.contains(node2), is(false));
                    }
                }
            }
        }
    }
}