/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.BackOff;

import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Per-server context information during a rolling restart/reconfigure
 */
public final class Context {
    /** The node this context refers to */
    private final NodeRef nodeRef;
    /** The process roles currently assigned to the node */
    private final NodeRoles currentRoles;
    /** The state of the node the last time it was observed */
    private State state;
    /** Whether it needs to be reattempted for a restart or reconfiguration */
    private boolean shouldRetry;
    /** The time of the last state transition */
    private long lastTransition;
    /** The reasons this node needs to be restarted or reconfigured */
    private final RestartReasons reason;
    /** The number of operational attempts so far. */
    private final BackOff backOff;

    private Context(NodeRef nodeRef, NodeRoles currentRoles, State state, long lastTransition, RestartReasons reason, BackOff backOff) {
        this.nodeRef = nodeRef;
        this.currentRoles = currentRoles;
        this.state = state;
        this.lastTransition = lastTransition;
        this.reason = reason;
        this.backOff = backOff;
    }

    static Context start(NodeRef nodeRef,
                         NodeRoles nodeRoles,
                         Function<Pod, RestartReasons> predicate,
                         Supplier<BackOff> backOffSupplier,
                         PodOperator podOperator,
                         String namespace,
                         Time time) {
        Pod pod = podOperator.get(namespace, nodeRef.podName());
        if (pod == null) {
            //TODO: debug logging here?
            return new Context(nodeRef, nodeRoles, State.UNKNOWN, time.systemTimeMillis(),  RestartReasons.empty(), backOffSupplier.get());
        } else {
            BackOff backOff = backOffSupplier.get();
            backOff.delayMs();
            return new Context(nodeRef, nodeRoles, State.UNKNOWN, time.systemTimeMillis(), predicate.apply(pod), backOff);
        }
    }

    State transitionTo(State state, Time time) {
        if (this.state() == state) {
            return state;
        }
        this.state = state;

        this.lastTransition = time.systemTimeMillis();
        return state;
    }

    void setRetryFlag(boolean shouldRetry) {
        this.shouldRetry = shouldRetry;
    }

    public int nodeId() {
        return nodeRef.nodeId();
    }

    public NodeRef nodeRef() {
        return nodeRef;
    }

    public NodeRoles currentRoles() {
        return currentRoles;
    }

    public State state() {
        return state;
    }
    public boolean shouldRetry() {
        return shouldRetry;
    }

    public long lastTransition() {
        return lastTransition;
    }

    public RestartReasons reason() {
        return reason;
    }

    public BackOff backOff() {
        return backOff;
    }

    @Override
    public String toString() {

        return "Context[" +
                "nodeRef=" + nodeRef + ", " +
                "currentRoles=" + currentRoles + ", " +
                "state=" + state + ", " +
                "lastTransition=" + Instant.ofEpochMilli(lastTransition) + ", " +
                "reason=" + reason + ']';
    }
}
