package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.Config;

/**
 * Holds Kafka broker configs and logger configs returned from Kafka Admin API.
 * @param nodeConfigs
 * @param nodeLoggerConfigs The replicas on this server
 */
record Configs(Config nodeConfigs, Config nodeLoggerConfigs) {

}

