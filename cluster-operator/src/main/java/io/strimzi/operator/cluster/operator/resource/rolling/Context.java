/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;

import java.time.Instant;
import java.util.function.Function;

/**
 * Per-server context information during a rolling restart/reconfigure
 */
final class Context {
    /** The node this context refers to */
    private final NodeRef nodeRef;
    /** The state of the node the last time it was observed */
    private State state;
    /** The time of the last state transition */
    private long lastTransition;
    /** The reasons this node needs to be restarted or reconfigured */
    private RestartReasons reason;
    /** The number of restarts attempted so far. */
    private int numRestarts;
    /** The number of reconfigurations attempted so far. */
    private int numReconfigs;
    /** The difference between the current logging config and the desired logging config */
    private KafkaBrokerLoggingConfigurationDiff loggingDiff;
    /** The difference between the current node config and the desired node config */
    private KafkaBrokerConfigurationDiff brokerConfigDiff;

    private Context(NodeRef nodeRef, State state, long lastTransition, RestartReasons reason, int numRestarts) {
        this.nodeRef = nodeRef;
        this.state = state;
        this.lastTransition = lastTransition;
        this.reason = reason;
        this.numRestarts = numRestarts;
    }

    static Context start(NodeRef nodeRef, Function<Integer, RestartReasons> predicate, Time time) {
        return new Context(nodeRef, State.UNKNOWN, time.systemTimeMillis(), predicate.apply(nodeRef.nodeId()), 0);
    }

    State transitionTo(State state, Time time) {
        if (this.state() == state) {
            return state;
        }
        this.state = state;
        if (state == State.RESTARTED) {
            this.numRestarts++;
        }
        if (state == State.RECONFIGURED) {
            this.numReconfigs++;
        }
        this.lastTransition = time.systemTimeMillis();
        return state;
    }

    public int serverId() {
        return nodeRef.nodeId();
    }

    public NodeRef nodeRef() {
        return nodeRef;
    }

    public State state() {
        return state;
    }

    public long lastTransition() {
        return lastTransition;
    }

    public RestartReasons reason() {
        return reason;
    }

    public int numRestarts() {
        return numRestarts;
    }

    public int numReconfigs() {
        return numReconfigs;
    }

    @Override
    public String toString() {

        return "Context[" +
                "nodeRef=" + nodeRef + ", " +
                "state=" + state + ", " +
                "lastTransition=" + Instant.ofEpochMilli(lastTransition) + ", " +
                "reason=" + reason + ", " +
                "numRestarts=" + numRestarts + ']';
    }

    public void brokerConfigDiff(KafkaBrokerConfigurationDiff diff) {
        this.brokerConfigDiff = diff;
    }

    public void loggingDiff(KafkaBrokerLoggingConfigurationDiff loggingDiff) {
        this.loggingDiff = loggingDiff;
    }

    public KafkaBrokerLoggingConfigurationDiff loggingDiff() {
        return loggingDiff;
    }

    public KafkaBrokerConfigurationDiff brokerConfigDiff() {
        return brokerConfigDiff;
    }
}
