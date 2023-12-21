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

    enum NodeState {
        /**
         * The pod/process is not running. This includes
         * <li>the pod has {@code status.phase=="Pending"} and condition {@code c} in {@code status.conditions}
         * with {@code c.type=="PodScheduled" && c.status=="False" && c.reason=="Unschedulable"}</li>
         * <li>Any of the containers in the waiting state with ready ImagePullBackoff or CrashLoopBackoff</li>
         */
        NOT_RUNNING,
        /** The pod/process is not {@link #NOT_RUNNING}, but is lacks a "Ready" condition with status "True" */
        NOT_READY,
        /** The pod/process is running and ready */
        READY
    }

    /** @return true if the pod for this node is not ready according to kubernetes */
    NodeState nodeState(NodeRef nodeRef);

    /**
     * Delete the pod with the given name, thus causing the restart of the corresponding Kafka server.
     * @param nodeRef The node.
     */
    public void restartNode(NodeRef nodeRef);

    /** @return Kafka process roles for this node */
    NodeRoles nodeRoles(NodeRef nodeRef);
}
