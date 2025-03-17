/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.CaConfig;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.concurrent.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static io.strimzi.operator.common.model.Ca.CA_KEY;
import static io.strimzi.operator.common.model.InternalCa.CA_STORE;
import static io.strimzi.operator.common.model.InternalCa.CA_STORE_PASSWORD;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CertManagerCaProviderTest {
    private static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    private static final String NAME = Reconciliation.DUMMY_RECONCILIATION.name();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
            .withName(NAME)
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .withListeners(new GenericKafkaListenerBuilder()
                    .withName("plain")
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
                    .build())
            .endKafka()
            .endSpec()
            .build();

    private final static OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();
    private final static PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private SecretOperator secretOperations;
    private CertManagerCertificateOperator certificateOperator;

    @BeforeEach
    public void setup() {
        secretOperations = mock(SecretOperator.class);
        certificateOperator = mock(CertManagerCertificateOperator.class);
    }

    private void reconcileCas(CertificateAuthority clusterCa, CertificateAuthority clientsCa, CaSecrets caSecrets, Secret clusterOperatorSecret) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                .withClusterCa(clusterCa)
                .withClientsCa(clientsCa)
                .endSpec()
                .build();

        reconcileCas(kafka, caSecrets, clusterOperatorSecret);
    }

    private void reconcileCas(Kafka kafka, CaSecrets caSecrets, Secret clusterOperatorSecret) {
        CertManagerCaProvider clusterCaProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(kafka.getSpec().getClusterCa(), false),
                kafka,
                caSecrets == null ? null : caSecrets.clusterCaCert,
                secretOperations,
                clusterOperatorSecret == null ? null : clusterOperatorSecret,
                kafka.getSpec().getClusterCa().getCertManager(),
                certificateOperator
        );

        clusterCaProvider.createCa().toCompletableFuture().join();
        clusterCaProvider.reconcileCaSecrets().toCompletableFuture().join();

        CertManagerCaProvider clientsCaProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(kafka.getSpec().getClientsCa(), false),
                kafka,
                caSecrets == null ? null : caSecrets.clientsCaCert,
                secretOperations,
                clusterOperatorSecret == null ? null : clusterOperatorSecret,
                kafka.getSpec().getClientsCa().getCertManager(),
                certificateOperator
        );

        clientsCaProvider.createCa().toCompletableFuture().join();
        clientsCaProvider.reconcileCaSecrets().toCompletableFuture().join();
    }

    private CertAndKey generateCa(CertificateAuthority certificateAuthority, String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        String clusterCaStorePassword = "123456";

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();
        Path clusterCaStoreFile = Files.createTempFile("tls", "cluster-ca-store");
        clusterCaStoreFile.toFile().deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(commonName).build();

        CERT_ISSUER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, certificateAuthority.getValidityDays());

        CERT_ISSUER.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                Files.readAllBytes(clusterCaStoreFile),
                null,
                clusterCaStorePassword);
    }

    private List<Secret> initialClusterCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "cluster-ca",
                AbstractModel.clusterCaKeySecretName(NAME),
                AbstractModel.clusterCaCertSecretName(NAME));
    }

    private List<Secret> initialClientsCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "clients-ca",
                KafkaResources.clientsCaKeySecretName(NAME),
                KafkaResources.clientsCaCertificateSecretName(NAME));
    }

    private List<Secret> initialCaSecrets(CertificateAuthority certificateAuthority, String commonName, String caKeySecretName, String caCertSecretName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey result = generateCa(certificateAuthority, commonName);
        Secret caKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, caKeySecretName, result.keyAsBase64String());
        Secret caCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, caCertSecretName,
                result.certAsBase64String(), result.trustStoreAsBase64String(), result.storePasswordAsBase64String());

        assertCertDataNotNull(caCertSecret.getData());
        assertThat(isCertInTrustStore(CA_CRT, caCertSecret.getData()), is(true));
        assertKeyDataNotNull(caKeySecret.getData());
        return List.of(caKeySecret, caCertSecret);
    }

    private KeyStore getTrustStore(Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                        Util.decodeBytesFromBase64(data.get(CA_STORE))),
                Util.decodeFromBase64(data.get(CA_STORE_PASSWORD)).toCharArray()
        );
        return trustStore;
    }

    private boolean isCertInTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return trustStore.isCertificateEntry(alias);
    }

    private X509Certificate getCertificateFromTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return (X509Certificate) trustStore.getCertificate(alias);
    }

    private void assertCaptorSecretsNotNull(CaSecrets secrets) {
        assertThat(secrets.clusterCaCert(), is(notNullValue()));
        assertThat(secrets.clientsCaCert(), is(notNullValue()));
    }

    private CaSecrets verifyCaSecretReconcileCalls(SecretOperator secretOps) {
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());

        return new CaSecrets(clusterCaCert.getValue(), clientsCaCert.getValue());
    }

    private void assertCertDataNotNull(Map<String, String> certData) {
        assertThat(certData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(certData.get(CA_CRT), is(notNullValue()));
        assertThat(certData.get(CA_STORE), is(notNullValue()));
        assertThat(certData.get(CA_STORE_PASSWORD), is(notNullValue()));
    }

    private void assertKeyDataNotNull(Map<String, String> keyData) {
        assertThat(keyData.keySet(), is(singleton(CA_KEY)));
        assertThat(keyData.get(CA_KEY), is(notNullValue()));
    }

    private record CaSecrets(
            Secret clusterCaCert,
            Secret clientsCaCert
    ) { }

//    @Test
//    public void testReconcileCMCasWhenClusterCaCertMissingThrows() {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                    .withNewCaCert()
//                        .withSecretName(clusterCaSecretName)
//                        .withCertificate(CA_CRT)
//                    .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertManagerCaProvider clusterCaProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
//                Ca.CaRole.CLUSTER_CA,
//                new CaConfig(clusterCa, false),
//                KAFKA,
//                null,
//                secretOperations,
//                clusterOperatorSecret == null ? null : clusterOperatorSecret,
//                kafka.getSpec().getClusterCa().getCertManager(),
//                certificateOperator
//        );
//
//        clusterCaProvider.createCa().toCompletableFuture().join();
//        reconcileCas(clusterCa, clientsCa, null, null)
//                .onComplete(context.failing(e -> context.verify(() -> {
//                    assertThat(e.getMessage(), is("CA public certificate Secret " + clusterCaSecretName + " missing."));
//    }
//
//    @Test
//    public void testReconcileCMCasWhenClientsCaCertMissingThrows(VertxTestContext context) {
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(true)
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.failing(e -> context.verify(() -> {
//                    assertThat(e.getMessage(), is("CA public certificate Secret " + clientsCaSecretName + " missing."));
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasWhenClusterCaCertKeyMissingThrows(VertxTestContext context) {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(true)
//                .build();
//
//        secrets.add(createSecret(clusterCaSecretName, Map.of(), Map.of()));
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.failing(e -> context.verify(() -> {
//                    assertThat(e.getMessage(), is("CA public certificate Secret " + clusterCaSecretName + " missing key " + CA_CRT));
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasWhenClientsCaCertKeyMissingThrows(VertxTestContext context) {
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(true)
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        secrets.add(createSecret(clientsCaSecretName, Map.of(), Map.of()));
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.failing(e -> context.verify(() -> {
//                    assertThat(e.getMessage(), is("CA public certificate Secret " + clientsCaSecretName + " missing key " + CA_CRT));
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasInitially(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "cert-manager-ca");
//        Secret initialClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, initialClusterCa.certAsBase64String()), Map.of());
//
//        CertAndKey initialClientsCa = generateCa(clientsCa, "cert-manager-ca");
//        Secret initialClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, initialClientsCa.certAsBase64String()), Map.of());
//
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClusterCa.cert()))));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasNoChange(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "cert-manager-ca");
//        Secret initialUserManagedClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, initialClusterCa.certAsBase64String()), Map.of());
//        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), initialClusterCa.certAsBase64String(), true);
//
//        CertAndKey initialClientsCa = generateCa(clientsCa, "cert-manager-ca");
//        Secret initialUserManagedClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, initialClientsCa.certAsBase64String()), Map.of());
//        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), initialClientsCa.certAsBase64String(), false);
//
//        CertAndKey clusterOperatorCertAndKey = generateClusterOperatorSignedCert(initialClusterCa, clusterCa.getValidityDays());
//        Secret clusterOperatorSecret = createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
//                Map.of("cluster-operator.crt", clusterOperatorCertAndKey.certAsBase64String(),
//                        "cluster-operator.key", clusterOperatorCertAndKey.keyAsBase64String()),
//                Labels.forStrimziCluster(NAME).withStrimziKind(Kafka.RESOURCE_KIND).toMap());
//
//        secrets.add(initialUserManagedClusterCaCertSecret);
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(initialUserManagedClientsCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//        secrets.add(clusterOperatorSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClusterCa.cert()))));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasNewCaCert(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey renewedClusterCa = renewCaCert(initialClusterCa, clusterCa.getValidityDays());
//        CertAndKey initialClientsCa = generateCa(clientsCa, "ca");
//        CertAndKey renewedClientsCa = renewCaCert(initialClientsCa, clientsCa.getValidityDays());
//
//        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), initialClusterCa.certAsBase64String(), true);
//        Secret renewedClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, renewedClusterCa.certAsBase64String()), Map.of());
//
//        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), initialClientsCa.certAsBase64String(), false);
//        Secret renewedClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, renewedClientsCa.certAsBase64String()), Map.of());
//
//        CertAndKey clusterOperatorCertAndKey = generateClusterOperatorSignedCert(initialClusterCa, clusterCa.getValidityDays());
//        Secret clusterOperatorSecret = createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
//                Map.of("cluster-operator.crt", clusterOperatorCertAndKey.certAsBase64String(),
//                        "cluster-operator.key", clusterOperatorCertAndKey.keyAsBase64String()),
//                Labels.forStrimziCluster(NAME).withStrimziKind(Kafka.RESOURCE_KIND).toMap());
//
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(renewedClusterCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//        secrets.add(renewedClientsCaCertSecret);
//        secrets.add(clusterOperatorSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClusterCa.cert()))));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(renewedClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(renewedClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasNewCaKeyAndCert(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey renewedClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey initialClientsCa = generateCa(clientsCa, "ca");
//        CertAndKey renewedClientsCa = generateCa(clientsCa, "ca");
//
//        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), initialClusterCa.certAsBase64String(), true);
//        Secret renewedClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, renewedClusterCa.certAsBase64String()), Map.of());
//
//        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), initialClientsCa.certAsBase64String(), false);
//        Secret renewedClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, renewedClientsCa.certAsBase64String()), Map.of());
//
//        CertAndKey clusterOperatorCertAndKey = generateClusterOperatorSignedCert(initialClusterCa, clusterCa.getValidityDays());
//        Secret clusterOperatorSecret = createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
//                Map.of("cluster-operator.crt", clusterOperatorCertAndKey.certAsBase64String(),
//                        "cluster-operator.key", clusterOperatorCertAndKey.keyAsBase64String()),
//                Labels.forStrimziCluster(NAME).withStrimziKind(Kafka.RESOURCE_KIND).toMap());
//
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(renewedClusterCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//        secrets.add(renewedClientsCaCertSecret);
//        secrets.add(clusterOperatorSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClusterCa.cert()))));
//                    Map<String, String> clusterCaCertData = clusterCaCert.getValue().getData();
//                    assertThat(clusterCaCertData, is(aMapWithSize(2)));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(renewedClusterCaCertSecret.getData().get(CA_CRT)));
//                    clusterCaCertData.remove(CA_CRT);
//                    Pattern oldCaCertKeyPattern = Pattern.compile("ca-[0-9]+-[0-9]+-[0-9]+T[0-9]+-[0-9]+-[0-9]+Z\\.crt");
//                    String oldCaCertKey = clusterCaCertData.keySet().stream().findFirst().orElse("");
//                    assertThat(oldCaCertKeyPattern.matcher(oldCaCertKey).matches(), is(true));
//                    assertThat(clusterCaCertData.get(oldCaCertKey), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(renewedClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasNewCaCertMissingClusterOperatorSecret(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey renewedClusterCa = renewCaCert(initialClusterCa, clusterCa.getValidityDays());
//        CertAndKey initialClientsCa = generateCa(clientsCa, "ca");
//        CertAndKey renewedClientsCa = renewCaCert(initialClientsCa, clientsCa.getValidityDays());
//
//        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), initialClusterCa.certAsBase64String(), true);
//        Secret renewedClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, renewedClusterCa.certAsBase64String()), Map.of());
//
//        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), initialClientsCa.certAsBase64String(), false);
//        Secret renewedClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, renewedClientsCa.certAsBase64String()), Map.of());
//
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(renewedClusterCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//        secrets.add(renewedClientsCaCertSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    // Since cluster operator Secret is missing we can't perform path validation, so cluster CA is not updated
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClusterCa.cert()))));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(renewedClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
//
//    @Test
//    public void testReconcileCMCasNewCaKeyAndCertMissingClusterOperatorSecret(VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
//        String clusterCaSecretName = "cert-manager-cluster-ca-cert";
//        String clientsCaSecretName = "cert-manager-clients-ca-cert";
//        CertificateAuthority clusterCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clusterCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertificateAuthority clientsCa = new CertificateAuthorityBuilder()
//                .withValidityDays(100)
//                .withRenewalDays(10)
//                .withGenerateCertificateAuthority(false)
//                .withType(CertificateManagerType.CERT_MANAGER_IO)
//                .withNewCertManager()
//                .withNewCaCert()
//                .withSecretName(clientsCaSecretName)
//                .withCertificate(CA_CRT)
//                .endCaCert()
//                .endCertManager()
//                .build();
//
//        CertAndKey initialClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey renewedClusterCa = generateCa(clusterCa, "ca");
//        CertAndKey initialClientsCa = generateCa(clientsCa, "ca");
//        CertAndKey renewedClientsCa = generateCa(clientsCa, "ca");
//
//        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), initialClusterCa.certAsBase64String(), true);
//        Secret renewedClusterCaCertSecret = createSecret(clusterCaSecretName, Map.of(CA_CRT, renewedClusterCa.certAsBase64String()), Map.of());
//
//        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), initialClientsCa.certAsBase64String(), false);
//        Secret renewedClientsCaCertSecret = createSecret(clientsCaSecretName, Map.of(CA_CRT, renewedClientsCa.certAsBase64String()), Map.of());
//
//        secrets.add(initialClusterCaCertSecret);
//        secrets.add(renewedClusterCaCertSecret);
//        secrets.add(initialClientsCaCertSecret);
//        secrets.add(renewedClientsCaCertSecret);
//
//        Checkpoint async = context.checkpoint();
//        reconcileCas(clusterCa, clientsCa)
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
//                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
//                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
//                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
//
//                    // Since cluster operator Secret is missing we can't perform path validation, so cluster CA is not updated
//                    assertThat(clusterCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clusterCaAnnotations = clusterCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
//                    assertThat(clusterCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(initialClusterCa.cert()))));
//                    assertThat(clusterCaCert.getValue().getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
//
//                    assertThat(clientsCaCert.getValue(), is(notNullValue()));
//                    Map<String, String> clientsCaAnnotations = clientsCaCert.getValue().getMetadata().getAnnotations();
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
//                    assertThat(clientsCaAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
//                    assertThat(clientsCaAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(CertUtils.getCertificateThumbprint(CaUtils.x509Certificate(renewedClientsCa.cert()))));
//                    assertThat(clientsCaCert.getValue().getData().get(CA_CRT), is(renewedClientsCaCertSecret.getData().get(CA_CRT)));
//
//                    async.flag();
//                })));
//    }
}
