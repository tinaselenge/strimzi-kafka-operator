/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over the platform (i.e. kubernetes).
 */
public interface PlatformClient {

    /**
     * State of the pod
     */
    enum PodState {
        /**
         * The pod/process is not running. This includes
         * the pod has {@code status.phase=="Pending"} and condition {@code c} in {@code status.conditions}
         * with {@code c.type=="PodScheduled" && c.status=="False" && c.reason=="Unschedulable"}
         * and any of the containers in the waiting state with ready ImagePullBackoff or CrashLoopBackoff
         */
        NOT_RUNNING,
        /** The pod/process is not {@link #NOT_RUNNING}, but is lacks a "Ready" condition with status "True" */
        NOT_READY,
        /** The pod/process is running and ready */
        READY
    }

    /**
     * Get state of the Pod
     * @param nodeRef Node reference
     * @return NodeState according to the platform
     * */
    PodState podState(NodeRef nodeRef);

    /**
     * Check pod readiness
     * @param nodeRef Node reference
     * @return CompletableFuture that completes when pod reaches readiness or times out
     * */
    CompletableFuture<Void> podReadiness(NodeRef nodeRef);

    /**
     * Initiate the restart of the corresponding Kafka server.
     *
     * @param nodeRef The node.
     * @param reasons Reasons for restarting the node to emit as an event
     *
     * @return CompletableFuture that completes when pod gets restarted
     */
    CompletableFuture<Void> restartPod(NodeRef nodeRef, RestartReasons reasons);

    /**
     * The current Kafka process roles of the node could differ from the NodeRef which is actually desired roles.
     *
     * @param nodeRef Node reference
     * @return Kafka process roles for this Kafka node according to the platform.
     */
    KafkaNodeRoles kafkaNodeRoles(NodeRef nodeRef);
}