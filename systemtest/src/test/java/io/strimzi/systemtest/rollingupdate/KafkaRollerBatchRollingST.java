/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;

@Tag(REGRESSION)
@Tag(ROLLING_UPDATE)
public class KafkaRollerBatchRollingST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerBatchRollingST.class);

    @ParallelNamespaceTest
    void testKafkaBatchRollingUpdates() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        setupClusterOperatorWithBatchConfiguration(3);

        final int brokerNodes = 9;
        final int controllerNodes = 1;

        LOGGER.info("Deploying KRaft cluster with dedicated controllers ({} replicas) and brokers ({} replicas).", controllerNodes, brokerNodes);

        // Create dedicated controller and broker KafkaNodePools and Kafka CR
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(),
                        testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerNodes).build(),
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(),
                        testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerNodes).build(),
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerNodes).build()
        );

        Map<String, String> brokerPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector());

        LOGGER.info("Create kafkaTopic: {}/{} with replica of 5 on each broker", testStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 3, 3, 2).build();
        KubeResourceManager.get().createResourceWithWait(kafkaTopic);

        LOGGER.info("Modifying Kafka CR to enable auto.create.topics.enable=false, expecting rolling update of all nodes including controllers.");

        // change Broker-only configuration inside shared Kafka configuration between KafkaNodePools and see that only broker pods rolls
        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "initial.broker.registration.timeout.ms", 33500);

        //TODO: how to check that pods rolled in batch?
        // check restart times?
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector(), brokerNodes, brokerPoolPodsSnapshot);

        // The cluster should remain Ready and CO should not be stuck with TimeoutException
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    //TODO: If the config is for Kafka rather than CO, this test can be moved to KafkaRollerST rather than being a separate test class
    private void setupClusterOperatorWithBatchConfiguration(int batchSize) {
        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_MAX_RESTART_BATCH_SIZE_ENV, String.valueOf(batchSize), null));

        SetupClusterOperator
                .getInstance()
                .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                        .withExtraEnvVars(coEnvVars)
                        .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
                        .build()
                )
                .install();
    }
}
