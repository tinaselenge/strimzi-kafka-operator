/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.common.certmanager.IssuerKind;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRefBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.concurrent.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.operator.common.model.Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION;
import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CertManagerCaTest {
    private final static String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    private final static String ENTITY_NAME = "mock-component";
    private final static int VALIDITY_DAYS = 100;
    private final static int RENEWAL_DAYS = 10;
    private final static OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();

    private CertManagerCertificateOperator certManagerCertificateOperator;
    private SecretOperator secretOperator;

    @BeforeEach
    public void setup() throws IOException {
        certManagerCertificateOperator = mock(CertManagerCertificateOperator.class);
        secretOperator = mock(SecretOperator.class);
    }

    private void initKafkaReconcilerTestMocks(Secret certManagerSecret) {
        when(secretOperator.getAsync(eq(NAMESPACE), eq(certManagerSecret.getMetadata().getName()))).thenAnswer(i -> CompletableFuture.completedStage(certManagerSecret));

        when(certManagerCertificateOperator.reconcile(any(), eq(NAMESPACE), any(), any(Certificate.class))).thenAnswer(i -> CompletableFuture.completedStage(ReconcileResult.patched(i.getArgument(3))));
        when(certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), any())).thenReturn(CompletableFuture.completedStage(null));
    }

    private static CertificateAuthority getCertificateAuthority() {
        return new CertificateAuthorityBuilder()
                .withValidityDays(VALIDITY_DAYS)
                .withRenewalDays(RENEWAL_DAYS)
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .withNewCertManager()
                    .withNewCaCert()
                        .withSecretName("my-cluster-ca-secret")
                        .withCertificate(CA_CRT)
                    .endCaCert()
                    .withNewIssuerRef()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                    .endIssuerRef()
                .endCertManager()
                .build();
    }

    private Secret createSecret(String secretName, Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(data)
                .build();
    }

    private Secret createCaCertSecret(String secretName, Map<String, String> data, int caCertGeneration) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(secretName)
                .withNamespace(NAMESPACE)
                .withAnnotations(Map.of(ANNO_STRIMZI_IO_CA_CERT_GENERATION, Integer.toString(caCertGeneration)))
                .endMetadata()
                .withData(data)
                .build();
    }

    private CertAndKey generateCa(CertificateAuthority certificateAuthority) throws IOException {

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-ca").build();

        CERT_ISSUER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, certificateAuthority.getValidityDays());
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                null,
                null,
                null);
    }

    private CertAndKey renewCaCert(CertAndKey certAndKey) throws IOException {
        Path caKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        caKeyFile.toFile().deleteOnExit();
        Files.write(caKeyFile, certAndKey.key());
        Path caCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        caCertFile.toFile().deleteOnExit();
        Files.write(caCertFile, certAndKey.cert());

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-ca").build();

        CERT_ISSUER.renewSelfSignedCert(caKeyFile.toFile(), caCertFile.toFile(), sbj, 10);

        return new CertAndKey(
                Files.readAllBytes(caKeyFile),
                Files.readAllBytes(caCertFile),
                null,
                null,
                null);
    }

    private CertAndKey generateCert(CertAndKey ca) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        csrFile.deleteOnExit();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        keyFile.deleteOnExit();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        certFile.deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ENTITY_NAME).build();

        CERT_ISSUER.generateCsr(keyFile, csrFile, sbj);
        CERT_ISSUER.generateCert(csrFile, ca.key(), ca.cert(), certFile, sbj, 10);

        return new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                null,
                null);
    }

    @Test
    void generateSignedCert() {
        // Create cluster ca cert Secret
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put("ca.crt", MockCertIssuer.clusterCaCert());
        Secret clusterCaCertSecret = createCaCertSecret("my-cluster-ca-secret", clusterCaCertData, 0);

        // Create cert-manager Secret for entity cert as though Certificate request will succeed
        Map<String, String> cmSecretData = new HashMap<>();
        cmSecretData.put("tls.crt", Util.encodeToBase64(MockCertIssuer.serverCert()));
        cmSecretData.put("tls.key", Util.encodeToBase64(MockCertIssuer.serverKey()));
        Secret cmSecret = createSecret(ENTITY_NAME + "-cm", cmSecretData);

        initKafkaReconcilerTestMocks(cmSecret);

        CertManagerCa certManagerCa = new CertManagerCa(
                Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                clusterCaCertSecret,
                null,
                new CaConfig(getCertificateAuthority(), false),
                certManagerCertificateOperator,
                secretOperator,
                null,
                Labels.EMPTY,
                new IssuerRefBuilder()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                        .build()
                );

        Subject subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ENTITY_NAME)
                .addDnsName("mock-component.namespace.local")
                .addIpAddress("127.0.0.1")
                .build();

        certManagerCa.generateSignedCert(ENTITY_NAME, subject)
                .whenComplete((cert, throwable) -> {
                    assertNull(throwable);

                    // Certificate Object created
                    ArgumentCaptor<Certificate> entityCertificateResourceCaptor =  ArgumentCaptor.forClass(Certificate.class);
                    verify(certManagerCertificateOperator, times(1)).reconcile(any(), eq(NAMESPACE), eq(ENTITY_NAME), entityCertificateResourceCaptor.capture());

                    Certificate certificate = entityCertificateResourceCaptor.getValue();
                    assertThat(certificate.getSpec().getCommonName(), is(ENTITY_NAME));

                    assertThat(certificate.getSpec().getSubject().getOrganizations().size(), is(1));
                    assertThat(certificate.getSpec().getSubject().getOrganizations().getFirst(), is("io.strimzi"));

                    assertThat(certificate.getSpec().getDuration(), is(new io.fabric8.kubernetes.api.model.Duration(Duration.ofDays(VALIDITY_DAYS))));
                    assertThat(certificate.getSpec().getRenewBefore(), is(new io.fabric8.kubernetes.api.model.Duration(Duration.ofDays(RENEWAL_DAYS))));

                    assertThat(certificate.getSpec().getDnsNames().size(), is(1));
                    assertThat(certificate.getSpec().getDnsNames().getFirst(), is("mock-component.namespace.local"));

                    assertThat(certificate.getSpec().getIpAddresses().size(), is(1));
                    assertThat(certificate.getSpec().getIpAddresses().getFirst(), is("127.0.0.1"));

                    // Entity cert is returned
                    assertThat(cert.cert(), is(Util.decodeBytesFromBase64(cmSecretData.get("tls.crt"))));
                    assertThat(cert.key(), is(Util.decodeBytesFromBase64(cmSecretData.get("tls.key"))));
                }).toCompletableFuture().join();
    }

    @Test
    void generateCertWhenCertificateResourceCreationFails() {
        // Create cluster ca cert Secret
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put("ca.crt", MockCertIssuer.clusterCaCert());
        Secret clusterCaCertSecret = createCaCertSecret("my-cluster-ca-secret", clusterCaCertData, 0);

        when(certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), eq(ENTITY_NAME)))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("Timed out waiting for resource to be ready")));

        CertManagerCa certManagerCa = new CertManagerCa(
                Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                clusterCaCertSecret,
                null,
                new CaConfig(getCertificateAuthority(), false),
                certManagerCertificateOperator,
                secretOperator,
                null,
                Labels.EMPTY,
                new IssuerRefBuilder()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                        .build()
        );

        Subject subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ENTITY_NAME)
                .build();

        try {
            certManagerCa.generateSignedCert(ENTITY_NAME, subject).toCompletableFuture().join();
        } catch (Exception e) {
            assertThat(e.getMessage(), CoreMatchers.containsString("Timed out waiting for resource to be ready"));

            // Certificate Object created
            ArgumentCaptor<Certificate> entityCertificateResourceCaptor = ArgumentCaptor.forClass(Certificate.class);
            verify(certManagerCertificateOperator, times(1)).reconcile(any(), eq(NAMESPACE), eq(ENTITY_NAME), entityCertificateResourceCaptor.capture());

            assertThat(entityCertificateResourceCaptor.getValue().getSpec().getCommonName(), is(ENTITY_NAME));
        }
    }

    @Test
    void entityCertRenewed() throws IOException {
        // Create cluster ca cert Secret
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put("ca.crt", MockCertIssuer.clusterCaCert());
        Secret clusterCaCertSecret = createCaCertSecret("my-cluster-ca-secret", clusterCaCertData, 0);

        // Create cert-manager Secret for entity cert as though Certificate has been renewed
        CertAndKey renewedCert = generateCert(new CertAndKey(Util.decodeBytesFromBase64(MockCertIssuer.clusterCaKey()), Util.decodeBytesFromBase64(MockCertIssuer.clusterCaCert())));
        Map<String, String> cmSecretData = new HashMap<>();
        cmSecretData.put("tls.crt", renewedCert.certAsBase64String());
        cmSecretData.put("tls.key", renewedCert.keyAsBase64String());
        Secret cmSecret = createSecret(ENTITY_NAME + "-cm", cmSecretData);

        initKafkaReconcilerTestMocks(cmSecret);

        CertManagerCa certManagerCa = new CertManagerCa(
                Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                clusterCaCertSecret,
                null,
                new CaConfig(getCertificateAuthority(), false),
                certManagerCertificateOperator,
                secretOperator,
                null,
                Labels.EMPTY,
                new IssuerRefBuilder()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                        .build()
        );

        Subject subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ENTITY_NAME)
                .addDnsName("mock-component.namespace.local")
                .addIpAddress("127.0.0.1")
                .build();

        certManagerCa.maybeCopyOrGenerateCert(ENTITY_NAME, subject, new CertAndKey(MockCertIssuer.serverKey().getBytes(StandardCharsets.UTF_8), MockCertIssuer.serverCert().getBytes(StandardCharsets.UTF_8), 0))
                .whenComplete((cert, throwable) -> {
                    assertNull(throwable);

                    // Certificate Object created
                    ArgumentCaptor<Certificate> entityCertificateResourceCaptor =  ArgumentCaptor.forClass(Certificate.class);
                    verify(certManagerCertificateOperator, times(1)).reconcile(any(), eq(NAMESPACE), eq(ENTITY_NAME), entityCertificateResourceCaptor.capture());

                    assertThat(entityCertificateResourceCaptor.getValue().getSpec().getCommonName(), is(ENTITY_NAME));

                    // Entity cert Secret is returned
                    assertThat(cert.cert(), is(renewedCert.cert()));
                    assertThat(cert.key(), is(renewedCert.key()));
                    assertThat(cert.caCertGeneration(), is(0));
                }).toCompletableFuture().join();
    }

    @Test
    void clusterCaAndEntityCertRenewed() throws IOException {
        // Create renewed cluster ca cert Secret
        CertAndKey renewedCaCert = renewCaCert(new CertAndKey(Util.decodeBytesFromBase64(MockCertIssuer.clusterCaKey()), Util.decodeBytesFromBase64(MockCertIssuer.clusterCaCert())));
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put("ca.crt", renewedCaCert.certAsBase64String());
        Secret clusterCaCertSecret = createCaCertSecret("my-cluster-ca-secret", clusterCaCertData, 1);

        // Create cert-manager Secret for entity cert as though Certificate has been renewed with renewed Ca cert
        CertAndKey renewedCert = generateCert(renewedCaCert);
        Map<String, String> cmSecretData = new HashMap<>();
        cmSecretData.put("tls.crt", renewedCert.certAsBase64String());
        cmSecretData.put("tls.key", renewedCert.keyAsBase64String());
        Secret cmSecret = createSecret(ENTITY_NAME + "-cm", cmSecretData);

        initKafkaReconcilerTestMocks(cmSecret);

        CertManagerCa certManagerCa = new CertManagerCa(
                Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                clusterCaCertSecret,
                null,
                new CaConfig(getCertificateAuthority(), false),
                certManagerCertificateOperator,
                secretOperator,
                null,
                Labels.EMPTY,
                new IssuerRefBuilder()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                        .build()
        );

        Subject subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ENTITY_NAME)
                .addDnsName("mock-component.namespace.local")
                .addIpAddress("127.0.0.1")
                .build();

        certManagerCa.maybeCopyOrGenerateCert(ENTITY_NAME, subject, new CertAndKey(MockCertIssuer.serverKey().getBytes(StandardCharsets.UTF_8), MockCertIssuer.serverCert().getBytes(StandardCharsets.UTF_8), 0))
                .whenComplete((cert, throwable) -> {
                    assertNull(throwable);

                    // Certificate Object created
                    ArgumentCaptor<Certificate> entityCertificateResourceCaptor =  ArgumentCaptor.forClass(Certificate.class);
                    verify(certManagerCertificateOperator, times(1)).reconcile(any(), eq(NAMESPACE), eq(ENTITY_NAME), entityCertificateResourceCaptor.capture());

                    assertThat(entityCertificateResourceCaptor.getValue().getSpec().getCommonName(), is(ENTITY_NAME));

                    // Entity cert Secret is returned
                    assertThat(cert.cert(), is(renewedCert.cert()));
                    assertThat(cert.key(), is(renewedCert.key()));
                    assertThat(cert.caCertGeneration(), is(1));
                }).toCompletableFuture().join();
    }
}
