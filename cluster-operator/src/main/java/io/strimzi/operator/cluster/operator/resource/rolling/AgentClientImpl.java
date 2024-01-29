/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Secret;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.common.Reconciliation;

class AgentClientImpl implements AgentClient {
    private final KafkaAgentClient kafkaAgentClient;

    AgentClientImpl(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret) {
        this.kafkaAgentClient = new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), clusterCaCertSecret, coKeySecret);

    }

    @Override
    public BrokerState getBrokerState(NodeRef nodeRef) {
        String podName = nodeRef.podName();
        return BrokerState.fromValue((byte) kafkaAgentClient.getBrokerState(podName).code());
    }
}
