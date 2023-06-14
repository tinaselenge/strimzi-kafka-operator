/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.RestartReason;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RackRolling {
    /**
     * A replica, on a broker,
     * @param topicName The name of the topic
     * @param partitionId The partition id
     * @param isrSize If the broker hosting this replica is in the ISR for the partition of this replica
     *                this is the size of the ISR.
     *                If the broker hosting this replica is NOT in the ISR for the partition of this replica
     *                this is the negative of the size of the ISR.
     *                In other words, the magnitude is the size of the ISR and the sign will be negative
     *                if the broker hosting this replic is not in the ISR.
     */
    record Replica(String topicName, int partitionId, short isrSize) {

        public Replica(Node broker, String topicName, int partitionId, Collection<Node> isr) {
            this(topicName, partitionId, (short) (isr.contains(broker) ? isr.size() : -isr.size()));
        }

        @Override
        public String toString() {
            return topicName + "-" + partitionId;
        }

        /**
         * @return The size of the ISR for the partition of this replica.
         */
        public short isrSize() {
            return (short) Math.abs(isrSize);
        }

        /**
         * @return true if the broker hosting this replica is in the ISR for the partition of this replica.
         */
        public boolean isInIsr() {
            return isrSize > 0;
        }
    }

    record Server(int id, String rack, Set<Replica> replicas) {
    }

    private final static int ADMIN_BATCH_SIZE = 200;

    /** Return a future that completes when all of the given futures complete */
    @SuppressWarnings("rawtypes")
    private static CompletableFuture<Void> allOf(List<? extends CompletableFuture<? extends Object>> futures) {
        CompletableFuture[] ts = futures.toArray(new CompletableFuture[0]);
        return CompletableFuture.allOf(ts);
    }

    /** Splits the given {@code items} into batches no larger than {@code maxBatchSize}. */
    private static <T> Set<List<T>> batch(List<T> items, int maxBatchSize) {
        Set<List<T>> allBatches = new HashSet<>();
        List<T> currentBatch = null;
        for (var topicId : items) {
            if (currentBatch == null || currentBatch.size() > maxBatchSize) {
                currentBatch = new ArrayList<>();
                allBatches.add(currentBatch);
            }
            currentBatch.add(topicId);
        }
        return allBatches;
    }

    private static Stream<TopicDescription> describeTopics(Admin admin, List<Uuid> topicIds) throws InterruptedException, ExecutionException {
        var topicIdBatches = batch(topicIds, ADMIN_BATCH_SIZE);
        var futures = new ArrayList<CompletableFuture<Map<Uuid, TopicDescription>>>();
        for (var topicIdBatch : topicIdBatches) {
            var mapKafkaFuture = admin.describeTopics(TopicCollection.ofTopicIds(topicIdBatch)).allTopicIds().toCompletionStage().toCompletableFuture();
            futures.add(mapKafkaFuture);
        }
        allOf(futures).get();
        var topicDescriptions = futures.stream().flatMap(cf -> {
            try {
                return cf.get().values().stream();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        return topicDescriptions;
    }

    private static Map<String, Integer> describeTopicConfigs(Admin admin, List<String> topicNames) throws InterruptedException, ExecutionException {
        var topicIdBatches = batch(topicNames, ADMIN_BATCH_SIZE);
        var futures = new ArrayList<CompletableFuture<Map<ConfigResource, Config>>>();
        for (var topicIdBatch : topicIdBatches) {
            var mapKafkaFuture = admin.describeConfigs(topicIdBatch.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toSet())).all().toCompletionStage().toCompletableFuture();
            futures.add(mapKafkaFuture);
        }
        allOf(futures).get();
        var topicDescriptions = futures.stream().flatMap(cf -> {
            try {
                return cf.get().entrySet().stream();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        return topicDescriptions.collect(Collectors.toMap(
                entry -> entry.getKey().name(),
                entry -> Integer.parseInt(entry.getValue().get("min.isr").value())));
    }

    private static boolean wouldBeUnderReplicated(Integer minIsr, Replica replica) {
        final boolean wouldByUnderReplicated;
        if (minIsr == null) {
            // if topic doesn't have minISR then it's fine
            wouldByUnderReplicated = false;
        } else {
            // else topic has minISR
            // compute spare = size(ISR) - minISR
            int sizeIsr = replica.isrSize();
            int spare = sizeIsr - minIsr;
            if (spare > 0) {
                // if (spare > 0) then we can restart the broker hosting this replica
                // without the topic being under-replicated
                wouldByUnderReplicated = false;
            } else if (spare == 0) {
                // if we restart this broker this replica would be under-replicated if it's currently in the ISR
                // if it's not in the ISR then restarting the server won't make a difference
                wouldByUnderReplicated = replica.isInIsr();
            } else {
                // this partition is already under-replicated
                // if it's not in the ISR then restarting the server won't make a difference
                // but in this case since it's already under-replicated let's
                // not possible prolong the time to this server rejoining the ISR
                wouldByUnderReplicated = true;
            }
        }
        return wouldByUnderReplicated;
    }

    private static boolean avail(Server server,
                                 Map<String, Integer> minIsrByTopic) {
        for (var replica : server.replicas()) {
            var topicName = replica.topicName();
            Integer minIsr = minIsrByTopic.get(topicName);
            if (wouldBeUnderReplicated(minIsr, replica)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Split the given cells into batches,
     * taking account of {@code acks=all} availability and the given maxBatchSize
     */
    public static List<Set<Server>> batchCells(List<Set<Server>> cells,
                                               Map<String, Integer> minIsrByTopic,
                                               int maxBatchSize) {
        List<Set<Server>> result = new ArrayList<>();
        for (var cell : cells) {
            List<Set<Server>> availBatches = new ArrayList<>();
            Set<Server> unavail = new HashSet<>();
            for (var server : cell) {
                if (avail(server, minIsrByTopic)) {
                    var currentBatch = availBatches.isEmpty() ? null : availBatches.get(availBatches.size() - 1);
                    if (currentBatch == null || currentBatch.size() >= maxBatchSize) {
                        currentBatch = new HashSet<>();
                        availBatches.add(currentBatch);
                    }
                    currentBatch.add(server);
                } else {
                    unavail.add(server);
                }
            }
            result.addAll(availBatches);
        }
        return result;
    }

    static <T> boolean intersects(Set<T> set, Set<T> set2) {
        for (T t : set) {
            if (set2.contains(t)) {
                return true;
            }
        }
        return false;
    }

    static boolean containsAny(Set<Server> cell, Set<Replica> replicas) {
        for (var b : cell) {
            if (intersects(b.replicas(), replicas)) {
                return true;
            }
        }
        return false;
    }

    /** Returns a new set that is the union of each of the sets in the given {@code merge}. I.e. flatten without duplicates. */
    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }

    private static Set<Set<Server>> partitionByHasAnyReplicasInCommon(Set<Server> rollable) {
        Set<Set<Server>> disjoint = new HashSet<>();
        for (var server : rollable) {
            var replicas = server.replicas();
            Set<Set<Server>> merge = new HashSet<>();
            for (Set<Server> cell : disjoint) {
                if (!containsAny(cell, replicas)) {
                    merge.add(cell);
                    merge.add(Set.of(server));
                    // problem is here, we're iterating over all cells (ones which we've decided should be disjoint)
                    // and we merged them in violation of that
                    // we could break here at the end of the if block (which would be correct)
                    // but it might not be optimal (in the sense of forming large cells)
                    break;
                }
            }
            if (merge.isEmpty()) {
                disjoint.add(Set.of(server));
            } else {
                for (Set<Server> r : merge) {
                    disjoint.remove(r);
                }
                disjoint.add(union(merge));
            }
        }
        return disjoint;
    }

    /**
     * Partition the given {@code brokers}
     * into cells that can be rolled in parallel because they
     * contain no replicas in common.
     */
    public static List<Set<Server>> cells(Collection<Server> brokers) {
//        if (brokers.stream().allMatch(b -> b.rack != -1)) {
//            // we know the racks of all brokers
//        } else {
//            // we don't know the racks of some brokers => can't do rack-wise rolling
//        }

        // find brokers that are individually rollable
        var rollable = brokers.stream().collect(Collectors.toCollection(() ->
                new TreeSet<>(Comparator.comparing(Server::id))));
        if (rollable.size() < 2) {
            return List.of(rollable);
        } else {
            // partition the set under the equivalence relation "shares a partition with"
            Set<Set<Server>> disjoint = partitionByHasAnyReplicasInCommon(rollable);
            // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
            // We find the biggest set of brokers which can parallel-rolled
            var sorted = disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
            return sorted;
        }
    }



    /** Determine whether the given broker is rollable without affecting partition availability */
    static boolean isRollable(Server broker, Map<String, Integer> minIrs) {
        return true; // TODO
    }

    private static int activeController(Admin admin) throws InterruptedException, ExecutionException {
        // TODO when controllers not colocated with brokers, how do we find the active controller?
        DescribeClusterResult dcr = admin.describeCluster();
        var activeController = dcr.controller();
        int controllerId = activeController.get().id();
        return controllerId;
    }

    /**
     * Pick the "best" batch to be restarted.
     * This is the largest batch of available servers excluding the
     * @return the "best" batch to be restarted
     */
    public static Set<Server> pickBestBatchForRestart(List<Set<Server>> batches, int controllerId) {
        var sorted = batches.stream().sorted(Comparator.comparing(Set::size)).toList();
        if (sorted.size() == 0) {
            return Set.of();
        }
        if (sorted.size() > 1
                && sorted.get(0).stream().anyMatch(s -> s.id() == controllerId)) {
            return sorted.get(1);
        }
        return sorted.get(0);
    }

    private static Set<Server> nextBatch(Admin admin) throws ExecutionException, InterruptedException {


        // TODO figure out this KRaft stuff
//        var quorum = admin.describeMetadataQuorum().quorumInfo().get();
//        var controllers = quorum.voters().stream()
//                .map(QuorumInfo.ReplicaState::replicaId)
//                .map(controllerId -> {
//                    return new Server(controllerId, null, Set.of(new Replica("::__cluster_metadata", 0, true)));
//                }).toList();

        Collection<TopicListing> topicListings = admin.listTopics(new ListTopicsOptions().listInternal(true)).listings().get();

        var topicIds = topicListings.stream().map(TopicListing::topicId).toList();
        // batch the describeTopics requests to avoid trying to get the state of all topics in the cluster
        Stream<TopicDescription> topicDescriptions = describeTopics(admin, topicIds);
        Map<Integer, Server> servers = new HashMap<>();
        topicDescriptions.forEach(topicDescription -> {
            topicDescription.partitions().forEach(partition -> {
                partition.replicas().forEach(replicatingBroker -> {
                    var server = servers.computeIfAbsent(replicatingBroker.id(), ig -> new Server(replicatingBroker.id(), replicatingBroker.rack(), new HashSet<>()));
                    server.replicas().add(new Replica(
                            replicatingBroker,
                            topicDescription.name(),
                            partition.partition(),
                            partition.isr()));
                });
            });
        });

        var cells = cells(servers.values());

        // TODO filter each cell by brokers that actually need to be restarted

        int maxRollBatchSize = 10;
        var minIsrByTopic = describeTopicConfigs(admin, topicListings.stream().map(TopicListing::name).toList());
        var batches = batchCells(cells, minIsrByTopic, maxRollBatchSize);

        int controllerId = activeController(admin);
        var bestBatch = pickBestBatchForRestart(batches, controllerId);
        return bestBatch;
    }

    public static void main(String[] a) {
        int numRacks = 3;
        int numBrokers = 10;
        int numControllers = 5;
        int numTopics = 1;
        int numPartitionsPerTopic = 10;
        boolean coloControllers = true;
        int rf = 3;
        System.out.printf("numRacks = %d%n", numRacks);
        System.out.printf("numBrokers = %d%n", numBrokers);
        System.out.printf("numTopics = %d%n", numTopics);
        System.out.printf("numPartitionsPerTopic = %d%n", numPartitionsPerTopic);
        System.out.printf("rf = %d%n", rf);

        List<Server> servers = new ArrayList<>();
        for (int serverId = 0; serverId < (coloControllers ? Math.max(numBrokers, numControllers) : numControllers + numBrokers); serverId++) {
            Server server = new Server(serverId, Integer.toString(serverId % numRacks), new LinkedHashSet<>());
            servers.add(server);
            boolean isController = serverId < numControllers;
            if (isController) {
                server.replicas().add(new Replica("__cluster_metadata", 0, (short) numControllers));
            }
        }

        for (int topic = 1; topic <= numTopics; topic++) {
            for (int partition = 0; partition < numPartitionsPerTopic; partition++) {
                for (int replica = partition; replica < partition + rf; replica++) {
                    Server broker = servers.get((coloControllers ? 0 : numControllers) + replica % numBrokers);
                    broker.replicas().add(new Replica("t" + topic, partition, (short) rf));
                }
            }
        }

        for (var broker : servers) {
            System.out.println(broker);
        }

        // TODO validate

        var results = cells(servers);

        int group = 0;
        for (var result : results) {
            System.out.println("Group " + group + ": " + result.stream().map(b -> b.id()).collect(Collectors.toCollection(TreeSet::new)));
            group++;
        }
    }

    enum State {
        UNKNOWN, // the initial state
        NOT_READY, // decided to restart right now or broker state > 3
        RESTARTED, // after successful kube pod delete
        STARTING,  // /liveness endpoint 200? Or just from Pod status?
        RECOVERING, // broker state < 3
        SERVING, // broker state== 3
        LEADING_ALL_PREFERRED // broker state== 3 and leading all preferred replicas
    }

    static final class Context {
        private final int serverId;
        private State state;
        private long lastTransition;
        private String reason;
        private int numRestarts;

        private Context(int serverId, State state, long lastTransition, String reason, int numRestarts) {
            this.serverId = serverId;
            this.state = state;
            this.lastTransition = lastTransition;
            this.reason = reason;
            this.numRestarts = numRestarts;
        }

            static Context start(int serverId) {
                return new Context(serverId, State.UNKNOWN, System.currentTimeMillis(), null, 0);
            }

            void transitionTo(State state) {
                if (this.state() == state) {
                    return;
                }
                this.state = state;
                if (state == State.RESTARTED) {
                    this.numRestarts++;
                }
                this.lastTransition = System.currentTimeMillis();
            }

        public int serverId() {
            return serverId;
        }

        public State state() {
            return state;
        }

        public long lastTransition() {
            return lastTransition;
        }

        public String reason() {
            return reason;
        }

        public int numRestarts() {
            return numRestarts;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (Context) obj;
            return this.serverId == that.serverId &&
                    Objects.equals(this.state, that.state) &&
                    this.lastTransition == that.lastTransition &&
                    Objects.equals(this.reason, that.reason) &&
                    this.numRestarts == that.numRestarts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverId, state, lastTransition, reason, numRestarts);
        }

        @Override
        public String toString() {
            return "Context[" +
                    "serverId=" + serverId + ", " +
                    "state=" + state + ", " +
                    "lastTransition=" + lastTransition + ", " +
                    "reason=" + reason + ", " +
                    "numRestarts=" + numRestarts + ']';
        }

        }

    private static boolean isNotReady(Integer nodeId) {
        // TODO kube get the pod and check the status
    }

    private static int getBrokerState(Integer nodeId) {
        // TODO http get the broker state from the agent endpoint
    }

    private static State observe(Context context) {
        if (isNotReady(context.serverId())) {
            return State.NOT_READY;
        } else {
            try {
                int bs = getBrokerState(context.serverId());
                if (bs < 3) {
                    return State.RECOVERING;
                } else if (bs == 3) {
                    return State.SERVING;
                } else {
                    return State.NOT_READY;
                }
            } catch (Exception e) {
                return State.NOT_READY;
            }
        }
    }

    private static void restartPod(Context context) {
        if (context.numRestarts() > 3) {
            throw new RuntimeException("Too many restarts"); // TODO proper exception type
        }
        // TODO kube delete the pod
        context.transitionTo(State.RESTARTED);
        // TODO kube create an Event with the context.reason
    }

    private static void awaitState(Context context, State targetState) throws InterruptedException {
        while (true) {
            var state = observe(context);
            context.transitionTo(state);
            if (state == targetState) {
                return;
            }
            Thread.sleep(1000L); // TODO tidy this up
        }
    }

    private static void restartInParallel(Set<Context> batch) throws InterruptedException {
        for (Context context1 : batch) {
            restartPod(context1);
        }
        for (Context context : batch) {
            awaitState(context, State.SERVING);
        }
        for (Context context : batch) {
            // TODO elect preferred leaders
        }
    }

    private static void reconfigure(Context context) {
        // TODO Admin incrementalAlterConfig
        // TODO update context.state
        // TODO create kube Event
    }

    private static boolean isReconfigurable(Context context) {
        // TODO reuse existing logic to determining whether we can do a dynamic reconfiguration
        return false;
    }

    public static void rollingRestart(Admin admin, List<Integer> nodes, Function<Integer, Set<RestartReason>> predicate) throws InterruptedException {

        var contexts = nodes.stream().map(Context::start).toList();
        // Create contexts

        // Execute algo
        OUTER: while (true) {

            for (var context : contexts) {
                // restart unready brokers
                context.transitionTo(observe(context));
            }
            var byUnreadiness = contexts.stream().collect(Collectors.partitioningBy(context -> context.state() == State.NOT_READY);

            for (var context : byUnreadiness.get(true)) {
                context.reason = "Not ready";
                restartPod(context);
                awaitState(context, State.SERVING);
                // TODO elect preferred leaders
                continue OUTER;
            }

            var actable = byUnreadiness.get(false).stream().filter(context -> {
                var reasons = predicate.apply(context.serverId());
                if (reasons.isEmpty()) {
                    return false;
                } else {
                    context.reason = reasons.toString();
                    return true;
                }
            }).toList();

            var reconfigurable = actable.stream().collect(Collectors.partitioningBy(context -> isReconfigurable(context)));
            for (var context : reconfigurable.get(true)) {
                reconfigure(context);
                // TODO decide what our post-reconfigure checks are and wait for them to become true
                // TODO decide on parallel/batching dynamic reconfiguration
                // TODO prevent the same context being seen as reconfigurable each time around to loop (prevent non-termination)
                continue OUTER;
            }

            // TODO the rest of the new algorithm
            var batch = nextBatch(admin, reconfigurable.get(false));
            restartInParallel(batch);

            // termination condition
            if (contexts.stream().allMatch(context -> context.state() == State.LEADING_ALL_PREFERRED)) {
                break OUTER;
            }
        }
    }



}
