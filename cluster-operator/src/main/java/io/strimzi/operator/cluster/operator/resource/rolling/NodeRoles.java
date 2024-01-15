/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Holds process roles for node
 * @param controller set to true if the node has controller role
 * @param broker set to true if the node has broker role
 */
record NodeRoles(boolean controller, boolean broker) {
}

