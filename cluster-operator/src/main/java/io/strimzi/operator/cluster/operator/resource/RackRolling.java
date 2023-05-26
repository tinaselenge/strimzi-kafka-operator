/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class RackRolling {
    record Replica(String topicName, int partitionId, boolean inSync) {
        @Override
        public String toString() {
            return topicName + "-" + partitionId;
        }
    }

    record Broker(int id, int rack, Set<Replica> replicas) {
    }

    /** Return a set of brokers from the given list that can be restarted simultaneously without affecting partition availability */
    public static Set<Broker> rollable(Collection<Broker> brokers, Map<String, Integer> minIrs) {
//        if (brokers.stream().allMatch(b -> b.rack != -1)) {
//            // we know the racks of all brokers
//        } else {
//            // we don't know the racks of some brokers => can't do rack-wise rolling
//        }

        // find brokers that are individually rollable
        var rollable = brokers.stream().filter(broker -> isRollable(broker, minIrs)).collect(Collectors.toCollection(() ->
                new TreeSet<>(Comparator.comparing(Broker::id))));
        if (rollable.size() < 2) {
            return rollable;
        } else {
            // partition the set under the equivalence relation "shares a partition with"
            Set<Set<Broker>> disjoint = partitionByHasAnyReplicasInCommon(rollable);
            // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
            // We find the biggest set of brokers which can parallel-rolled
            var sorted = disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
            for (var s : sorted) {
                System.out.println("## " + s.stream().map(b -> b.id()).collect(Collectors.toCollection(TreeSet::new)));
            }
            Set<Broker> largest = sorted.get(0); // sorted is not empty because disjoint not empty
            return largest;
        }
    }

    private static Set<Set<Broker>> partitionByHasAnyReplicasInCommon(Set<Broker> rollable) {
        Set<Set<Broker>> disjoint = new HashSet<>();
        for (var broker : rollable) {
            var replicas = broker.replicas();
            Set<Set<Broker>> merge = new HashSet<>();
            for (Set<Broker> cell : disjoint) {
                if (!containsAny(cell, replicas)) {
                    merge.add(cell);
                    merge.add(Set.of(broker));
                }
            }
            if (merge.isEmpty()) {
                disjoint.add(Set.of(broker));
            } else {
                for (Set<Broker> r : merge) {
                    disjoint.remove(r);
                }
                disjoint.add(union(merge));
            }
        }
        return disjoint;
    }

    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }

    static boolean containsAny(Set<Broker> cell, Set<Replica> replicas) {
        for (var b : cell) {
            if (intersects(b.replicas(), replicas)) {
                return true;
            }
        }
        return false;
    }

    static <T> boolean intersects(Set<T> set, Set<T> set2) {
        for (T t : set) {
            if (set2.contains(t)) {
                return true;
            }
        }
        return false;
    }

    /** Determine whether the given broker is rollable without affecting partition availability */
    static boolean isRollable(Broker broker, Map<String, Integer> minIrs) {
        return true; // TODO
    }

    public static void main(String[] a) {
        int numRacks = 3;
        int numBrokers = 121;
        int numTopics = 1;
        int numPartitionsPerTopic = 500_000;
        int rf = 3;
        System.out.printf("numRacks = %d%n", numRacks);
        System.out.printf("numBrokers = %d%n", numBrokers);
        System.out.printf("numTopics = %d%n", numTopics);
        System.out.printf("numPartitionsPerTopic = %d%n", numPartitionsPerTopic);
        System.out.printf("rf = %d%n", rf);

        List<Broker> brokers = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            Broker broker = new Broker(brokerId, brokerId % numRacks, new LinkedHashSet<>());
            brokers.add(broker);
        }

        for (int topic = 1; topic <= numTopics; topic++) {
            for (int partition = 0; partition < numPartitionsPerTopic; partition++) {
                for (int replica = partition; replica < partition + rf; replica++) {
                    Broker broker = brokers.get(replica % numBrokers);
                    broker.replicas().add(new Replica("t" + topic, partition, true));
                }
            }
        }

        for (var broker : brokers) {
            System.out.println(broker);
        }

        // TODO validate

        var result = rollable(brokers, Map.of("t1", 2));

        System.out.println("" + result.stream().map(b -> b.id()).collect(Collectors.toCollection(TreeSet::new)));
    }


}
