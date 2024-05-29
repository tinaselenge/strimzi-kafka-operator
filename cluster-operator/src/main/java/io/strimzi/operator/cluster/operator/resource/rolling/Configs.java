/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.Config;

/**
 * Holds Kafka broker configs and logger configs returned from Kafka Admin API.
 * @param nodeConfigs
 * @param nodeLoggerConfigs The replicas on this server
 */
record Configs(Config nodeConfigs, Config nodeLoggerConfigs) {

}

