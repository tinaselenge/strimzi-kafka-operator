/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.QuorumInfo;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides methods to determine whether it's safe to restart a KRaft controller and identify the quorum leader id.
 * Restarting a KRaft controller is considered safe if the majority of controllers, excluding the one being
 * considered for restart, have caught up with the quorum leader within the specified timeout period defined by
 * controller.quorum.fetch.timeout.ms.
 */
class KafkaQuorumCheck2 {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaQuorumCheck2.class.getName());
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;

    private final Reconciliation reconciliation;
    private final KafkaRollerClient rollerClient;

    protected KafkaQuorumCheck2(Reconciliation reconciliation, KafkaRollerClient rollerClient) {
        this.reconciliation = reconciliation;
        this.rollerClient = rollerClient;
    }

    /**
     * Returns future that completes with true if the given controller can be rolled based on the quorum state. Quorum is considered
     * healthy if the majority of controllers, excluding the given node, have caught up with the quorum leader within the
     * controller.quorum.fetch.timeout.ms.
     */
    boolean canRollController(int nodeId) {
        LOGGER.debugCr(reconciliation, "Determining whether controller pod {} can be rolled", nodeId);
        QuorumInfo quorumInfo = rollerClient.describeMetadataQuorum();
        boolean canRoll = isQuorumHealthyWithoutNode(nodeId, quorumInfo);
        if (!canRoll) {
            LOGGER.debugCr(reconciliation, "Not restarting controller pod {}. Restart would affect the quorum health", nodeId);
        }
        return canRoll;
    }

    /**
     * Returns true if the majority of the controllers' lastCaughtUpTimestamps are within
     * the controller.quorum.fetch.timeout.ms based on the given quorum info.
     * The given nodeIdToRestart is the one being considered to restart, therefore excluded from the check.
     **/
    private boolean isQuorumHealthyWithoutNode(int nodeIdToRestart, QuorumInfo info) {
        int leaderId = info.leaderId();
        if (leaderId < 0) {
            LOGGER.warnCr(reconciliation, "No controller quorum leader is found because the leader id is set to {}", leaderId);
            return false;
        }

        Map<Integer, Long> controllerStates = info.voters().stream().collect(Collectors.toMap(
                QuorumInfo.ReplicaState::replicaId,
                state -> state.lastCaughtUpTimestamp().isPresent() ? state.lastCaughtUpTimestamp().getAsLong() : -1));
        int totalNumOfControllers = controllerStates.size();

        if (totalNumOfControllers == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with a single node. This may result in data loss " +
                    "or may cause disruption to the cluster during the rolling update. It is recommended that a minimum of three controllers are used.");
            return true;
        }

        long leaderLastCaughtUpTimestamp = controllerStates.get(leaderId);
        LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for the controller quorum leader (node id {}) is {}", leaderId, leaderLastCaughtUpTimestamp);

        long numOfCaughtUpControllers = controllerStates.entrySet().stream().filter(entry -> {
            int controllerNodeId = entry.getKey();
            long lastCaughtUpTimestamp = entry.getValue();
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", controllerNodeId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", controllerNodeId, lastCaughtUpTimestamp);
                if (controllerNodeId == leaderId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < getControllerQuorumFetchTimeoutMs(leaderId)) {

                    // skip the controller that we are considering to roll
                    if (controllerNodeId != nodeIdToRestart) {
                        return true;
                    }
                    LOGGER.debugCr(reconciliation, "Controller {} has caught up with the controller quorum leader", controllerNodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "Controller {} has fallen behind the controller quorum leader", controllerNodeId);
                }
            }
            return false;
        }).count();

        LOGGER.debugCr(reconciliation, "Out of {} controllers, there are {} that have caught up with the controller quorum leader, not including controller {}", totalNumOfControllers, numOfCaughtUpControllers, nodeIdToRestart);

        if (totalNumOfControllers == 2) {

            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. This may result in data loss  " +
                        "or cause disruption to the cluster during the rolling update. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            return numOfCaughtUpControllers >= (totalNumOfControllers + 2) / 2;
        }
    }

    private long getControllerQuorumFetchTimeoutMs(int leaderId) {
        long controllerQuorumFetchTimeout = CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;
        try {
            Config config = rollerClient.describeControllerConfigs(leaderId);
            if (config != null) {
                controllerQuorumFetchTimeout = Long.parseLong(config.get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME).value());
            }
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Error getting controller quorum fetch timeout for quorum leader {}. Using the default value of {} ", leaderId, e, controllerQuorumFetchTimeout);
        }
        return controllerQuorumFetchTimeout;
    }
}