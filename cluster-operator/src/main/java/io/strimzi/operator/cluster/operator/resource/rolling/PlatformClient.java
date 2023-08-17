/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;

public interface PlatformClient {
    /** @return true if the pod for this node is not ready according to kubernetes */
    boolean isNotReady(NodeRef nodeRef);

    /**
     * Delete the pod with the given name, thus causing the restart of the corresponding Kafka server.
     * @param nodeRef The node.
     */
    public void deletePod(NodeRef nodeRef);
}
