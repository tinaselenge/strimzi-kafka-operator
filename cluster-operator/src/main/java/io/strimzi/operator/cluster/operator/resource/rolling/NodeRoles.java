package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Holds process roles for node
 * @param controller set to true if the node has controller role
 * @param broker set to true if the node has broker role
 */
record NodeRoles(boolean controller, boolean broker) {
}

