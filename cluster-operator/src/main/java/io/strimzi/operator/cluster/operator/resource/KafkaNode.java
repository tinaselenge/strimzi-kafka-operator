package io.strimzi.operator.cluster.operator.resource;

import java.util.Set;

/**
 * Information about a Kafka node (which may be a broker, controller, or both) and its replicas.
 * @param id The id of the server
 * @param replicas The replicas on this server
 */
record KafkaNode(int id, Set<Replica> replicas) {
}