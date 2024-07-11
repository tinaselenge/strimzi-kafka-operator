/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Enumerates the possible "rolling states" of a Kafka node
 */
enum State {
    UNKNOWN, // the initial state or the node is successfully restarted/reconfigured
    NOT_RUNNING, // The pod/process is not running.
    NOT_READY, // decided to restart right now or broker state < 2 OR == 127
    RECOVERING, // broker state == 2
    SERVING, // broker state >= 3 AND != 127
    LEADING_ALL_PREFERRED // broker state== 3 and leading all preferred replicas
}
