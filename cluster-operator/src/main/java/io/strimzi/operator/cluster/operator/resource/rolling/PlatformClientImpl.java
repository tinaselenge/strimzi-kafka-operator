/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;

public class PlatformClientImpl implements PlatformClient {

    private final PodOperator podOps;
    private final String namespace;

    private final Reconciliation reconciliation;

    public PlatformClientImpl(PodOperator podOps, String namespace, Reconciliation reconciliation) {
        this.podOps = podOps;
        this.namespace = namespace;
        this.reconciliation = reconciliation;
    }

    @Override
    public boolean isNotReady(NodeRef nodeRef) {
        return !podOps.isReady(namespace, nodeRef.podName());
    }

    @Override
    public void restartNode(NodeRef nodeRef) {
        var pod = podOps.get(namespace, nodeRef.podName());
        podOps.restart(reconciliation, pod, 60_000);
    }
}
