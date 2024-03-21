/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchingTest {

    @Test
    public void testCellCreation() {

        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, true, false, addReplicas(0, "my-topic", 0, 1, 0)),
                        addKafkaNode(1, false, true, Set.of()),
                        addKafkaNode(2, false, true, addReplicas(2, "my-topic", 0, 1, 2))));

        assertEquals(cells.size(), 2);
        assertEquals(cells.get(0).toString(), "[KafkaNode[id=1, controller=false, broker=true, replicas=[]], KafkaNode[id=0, controller=true, broker=false, replicas=[my-topic-0]]]");
        assertEquals(cells.get(1).toString(), "[KafkaNode[id=2, controller=false, broker=true, replicas=[my-topic-0]]]");
    }

    @Test
    public void testBatchCreationWithSingleTopic() {

        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, true, false, addReplicas(0, "my-topic", 0, 1, 0)),
                        addKafkaNode(1, false, true, Set.of()),
                        addKafkaNode(2, false, true, addReplicas(2, "my-topic", 0, 1, 2))));

        Map<String, Integer> topicAndMinIsrs = new HashMap<>();

        topicAndMinIsrs.put("my-topic", 2);


        List<Set<KafkaNode>> batch = Batching.batchCells(Reconciliation.DUMMY_RECONCILIATION, cells, topicAndMinIsrs, 2);

        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0).toString(), "[KafkaNode[id=1, controller=false, broker=true, replicas=[]]]");
    }

    @Test
    public void testBatchCreationWithNonRestartableNodes() {

        Map<String, Integer> topicAndMinIsrs = new HashMap<>();

        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, true, false, addReplicas(0, "my-topic", 0, 1, 0)),
                        addKafkaNode(1, true, true, addReplicas(1, "my-topic", 1, 1, 0)),
                        addKafkaNode(2, false, true, addReplicas(2, "my-topic-1", 0, 1, 2)),
                        addKafkaNode(3, false, true, addReplicas(3, "my-topic-1", 0, 1, 3))));

        topicAndMinIsrs.put("my-topic", 2);
        topicAndMinIsrs.put("my-topic-1", 2);


        List<Set<KafkaNode>> batch = Batching.batchCells(Reconciliation.DUMMY_RECONCILIATION, cells, topicAndMinIsrs, 2);

        // cannot batch any cells since we cannot restart nodes {1,2,0,3} without violating some topics' min.in.sync.replicas
        assertEquals(batch.size(), 0);
    }

    @Test
    public void testBatchCreationWithMultipleTopics() {

        Map<String, Integer> topicAndMinIsrs = new HashMap<>();

        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, true, false, addReplicas(0, "my-topic", 0, 1, 0)),
                        addKafkaNode(1, true, true, addReplicas(1, "my-topic-1", 1, 1, 3)),
                        addKafkaNode(2, false, true, addReplicas(2, "my-topic-1", 1, 0, 2)),
                        addKafkaNode(3, false, true, addReplicas(3, "my-topic-2", 1, 1, 0))));

        topicAndMinIsrs.put("my-topic", 2);
        topicAndMinIsrs.put("my-topic-1", 2);
        topicAndMinIsrs.put("my-topic-2", 2);

        List<Set<KafkaNode>> batch = Batching.batchCells(Reconciliation.DUMMY_RECONCILIATION, cells, topicAndMinIsrs, 2);

        System.out.println(batch);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0).toString(), "[KafkaNode[id=3, controller=false, broker=true, replicas=[my-topic-2-1]]]");
    }

    static public Set<Replica> addReplicas(int brokerid, String topicName, int partition, int... isrIds) {
        Set<Replica> replicaSet = new HashSet<>();

        replicaSet.add(new Replica(addNodes(brokerid).get(0), topicName, partition, addNodes(isrIds)));

        return replicaSet;
    }

    static List<Node> addNodes(int... isrIds) {
        List<Node> nodes = new ArrayList<>();

        for (int id : isrIds) {
            nodes.add(new Node(id, "pool-kafka-" + id, 9092));
        }

        return nodes;
    }

    static public KafkaNode addKafkaNode(int nodeId, boolean controller, boolean broker, Set<Replica> replicas) {
        return new KafkaNode(nodeId, controller, broker, replicas);
    }
}
