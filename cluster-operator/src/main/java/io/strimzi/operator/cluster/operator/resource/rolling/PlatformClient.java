/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;

/**
 * Abstraction over the platform (i.e. kubernetes).
 */
public interface PlatformClient {

    /**
     * State of the node
     */
    enum NodeState {
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
     * @param nodeRef Node reference
     * @return true if the pod for this node is not ready according to the platform */
    NodeState nodeState(NodeRef nodeRef);

    /**
     * Initiate the restart of the corresponding Kafka server.
     * @param nodeRef The node.
     */
    void restartNode(NodeRef nodeRef);

    /**
     * @param nodeRef Node reference
     * @return Kafka process roles for this node according to the platform.
     * This could differ from the roles that the running process actually has (for instance if the process needs to be restarted to pick up its current roles).
     * */
    NodeRoles nodeRoles(NodeRef nodeRef);
}
