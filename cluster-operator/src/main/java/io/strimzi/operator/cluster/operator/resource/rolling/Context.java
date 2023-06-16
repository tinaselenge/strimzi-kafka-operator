/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Per-server context information during a rolling restart/reconfigure
 */
final class Context {
    private final int serverId;
    private State state;
    private long lastTransition;
    private String reason;
    private int numRestarts;
    private int numReconfigs;

    private Context(int serverId, State state, long lastTransition, String reason, int numRestarts) {
        this.serverId = serverId;
        this.state = state;
        this.lastTransition = lastTransition;
        this.reason = reason;
        this.numRestarts = numRestarts;
    }

    static Context start(int serverId) {
        return new Context(serverId, State.UNKNOWN, System.currentTimeMillis(), null, 0);
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
        return serverId;
    }

    public State state() {
        return state;
    }

    public long lastTransition() {
        return lastTransition;
    }

    public String reason() {
        return reason;
    }

    public void reason(String reason) {
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
                "serverId=" + serverId + ", " +
                "state=" + state + ", " +
                "lastTransition=" + lastTransition + ", " +
                "reason=" + reason + ", " +
                "numRestarts=" + numRestarts + ']';
    }

}
