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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Per-server context information during a rolling restart/reconfigure
 */
final class Context {
    private final NodeRef nodeRef;
    private State state;
    private long lastTransition;
    private RestartReasons reason;
    private int numRestarts;
    private int numReconfigs;
    private KafkaBrokerLoggingConfigurationDiff loggingDiff;
    private KafkaBrokerConfigurationDiff brokerConfigDiff;

    private Context(NodeRef nodeRef, State state, long lastTransition, RestartReasons reason, int numRestarts) {
        this.nodeRef = nodeRef;
        this.state = state;
        this.lastTransition = lastTransition;
        this.reason = reason;
        this.numRestarts = numRestarts;
    }

    static Context start(NodeRef nodeRef) {
        return new Context(nodeRef, State.UNKNOWN, System.currentTimeMillis(), null, 0);
    }

    State transitionTo(State state) {
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
        this.lastTransition = System.currentTimeMillis();
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

    public void reason(RestartReasons reason) {
        this.reason = reason;
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
                "lastTransition=" + DateTimeFormatter.ISO_DATE_TIME.format(Instant.ofEpochMilli(lastTransition).atZone(ZoneId.systemDefault())) + ", " +
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
