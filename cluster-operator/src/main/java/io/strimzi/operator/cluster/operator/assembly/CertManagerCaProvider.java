/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.certmanager.CertManager;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRef;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.CaConfig;
import io.strimzi.operator.common.model.CaUtils;
import io.strimzi.operator.common.model.CertManagerCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.operator.resource.concurrent.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;

import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.strimzi.operator.common.model.Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION;
import static io.strimzi.operator.common.model.Ca.CA_CRT;

public class CertManagerCaProvider extends CaProvider {
    private final SecretOperator secretOperator;
    private final Secret cluserOperatorSecret;
    private final CertManager certManagerConfig;
    private final CertificateAuthority certificateAuthority;
    private final CertManagerCertificateOperator certificateOperator;

    public CertManagerCaProvider(Reconciliation reconciliation, Ca.CaRole caRole, CaConfig caConfig, Kafka kafkaCr,
                                 Secret existingCaCertSecret, SecretOperator secretOperator,
                                 Secret clusterOperatorSecret, CertManager certManagerConfig,
                                 CertManagerCertificateOperator certificateOperator
                                 ) {
        super(reconciliation, caRole, caConfig, kafkaCr, existingCaCertSecret, null);
        this.secretOperator = secretOperator;
        this.cluserOperatorSecret = clusterOperatorSecret;
        this.certManagerConfig = certManagerConfig;
        this.certificateAuthority = switch (caRole) {
            case CLUSTER_CA -> kafkaCr.getSpec().getClusterCa();
            case CLIENTS_CA -> kafkaCr.getSpec().getClientsCa();
        };
        this.certificateOperator = certificateOperator;
    }

    @Override
    public CompletionStage<Ca> createCa() {
        return getCaCertForCertManager()
                .thenApply(newCaCertAsBase64 -> {
                    IssuerRef issuerRef = certificateAuthority != null && certificateAuthority.getCertManager() != null
                            ? certificateAuthority.getCertManager().getIssuerRef() : null;
                    CertManagerCa certManagerCa = new CertManagerCa(reconciliation, Ca.CaRole.CLUSTER_CA,
                            existingCaCertSecret,
                            existingCaKeySecret,
                            caConfig,
                            certificateOperator,
                            secretOperator,
                            caConfig.isGenerateSecretOwnerRef() ? new OwnerReferenceBuilder()
                                    .withApiVersion(kafkaCr.getApiVersion())
                                    .withKind(kafkaCr.getKind())
                                    .withName(kafkaCr.getMetadata().getName())
                                    .withUid(kafkaCr.getMetadata().getUid())
                                    .withBlockOwnerDeletion(true)
                                    .withController(false)
                                    .build()
                                    : null,
                            null,
                            issuerRef);
                    certManagerCa.createOrUpdateCa(
                            newCaCertAsBase64,
                            Annotations.stringAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, ""),
                            CaUtils.cert(cluserOperatorSecret, "cluster-operator.crt")
                    );
                    ca = certManagerCa;
                    return ca;
                });
    }

    @Override
    public CompletionStage<Secret> reconcileCaSecrets() {
        caCertSecret = createCertManagerCaCertSecret(caRole, ca.caCertData(),
                ca.caCertGeneration(), ca.caKeyGeneration());
        CompletableFuture<Secret> future = new CompletableFuture<>();
        secretOperator.reconcile(reconciliation, reconciliation.namespace(), caCertSecret.getMetadata().getName(), caCertSecret)
                .thenAccept(result -> {
                    future.complete(result.resource());
                });
        return future;
    }

    private CompletionStage<String> getCaCertForCertManager() {
        String caCertSecretName = certManagerConfig.getCaCert().getSecretName();
        String caCertSecretKey = certManagerConfig.getCaCert().getCertificate();
        return secretOperator.getAsync(reconciliation.namespace(), caCertSecretName)
                .thenApply(secret -> {
                    if (secret == null) {
                        throw new InvalidResourceException("CA public certificate Secret " + caCertSecretName + " missing.");
                    } else if (secret.getData().get(caCertSecretKey) == null) {
                        throw new InvalidResourceException("CA public certificate Secret " + caCertSecretName + " missing key " + caCertSecretKey);
                    }
                    validateUserCaCertChain(Map.of(caCertSecretKey, secret.getData().get(caCertSecretKey)));
                    return secret.getData().get(caCertSecretKey);
                });
    }

    private Secret createCertManagerCaCertSecret(Ca.CaRole caRole, Map<String, String> caCertData, int caCertGeneration, int caKeyGeneration) {
        Map<String, String> certAnnotations = new HashMap<>(2);

        try {
            certAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(Util.decodeBytesFromBase64(caCertData.get(CA_CRT)))));
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
        String secretName = switch (caRole) {
            case CLUSTER_CA -> {
                certAnnotations.put(ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(caKeyGeneration));
                yield AbstractModel.clusterCaCertSecretName(reconciliation.name());
            }
            case CLIENTS_CA -> KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
        };

        return createCaCertSecret(caRole, secretName, caCertData, certAnnotations, caCertGeneration);
    }
}
