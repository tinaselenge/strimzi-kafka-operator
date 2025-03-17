/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.CaConfig;
import io.strimzi.operator.common.model.InternalCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class UserCaProvider extends CaProvider {
    private final CertIssuer certIssuer;
    private final PasswordGenerator passwordGenerator;

    public UserCaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr, CertIssuer certIssuer, PasswordGenerator passwordGenerator, Secret existingCaCert, Secret existingCaKey) {
        super(reconciliation, caRole, caConfig, kafkaCr, existingCaCert, existingCaKey);
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;
    }

    @Override
    public CompletionStage<Ca> createCa() {
        if (existingCaCertSecret == null || existingCaKeySecret == null)   {
            throw new InvalidResourceException(caRole.name() + " should not be generated, but the secrets were not found.");
        }
        validateUserCaCertChain(existingCaCertSecret.getData());
        caCertSecret = existingCaCertSecret;
        caKeySecret = existingCaKeySecret;
        ca = new InternalCa(reconciliation, caRole, certIssuer, passwordGenerator, caCertSecret, caKeySecret, caConfig);
        return CompletableFuture.completedStage(ca);
    }

    @Override
    public CompletionStage<Secret> reconcileCaSecrets() {
        return CompletableFuture.completedStage(caCertSecret);
    }


}
