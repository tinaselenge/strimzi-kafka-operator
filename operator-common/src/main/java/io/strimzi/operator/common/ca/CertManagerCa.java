/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRef;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;

import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * A Certificate Authority managed by cert-manager
 */
@SuppressWarnings("checkstyle:CyclomaticComplexity")
public class CertManagerCa extends Ca {
    private static final String CERT_MANAGER_SECRET_SUFFIX = "-cm";
    private final CertManagerCertificateOperator certManagerCertificateOperator;
    private final SecretOperator secretOperator;
    private final OwnerReference ownerReference;
    private final Labels labels;
    protected final IssuerRef issuerRef;

    /**
     * Constructs the CA object
     *
     * @param reconciliation        Reconciliation marker
     * @param caRole                Ca role
     * @param caCertSecret          Kubernetes Secret where the CA public key is stored
     * @param caConfig              Certificate Authority configuration
     * @param certManagerCertificateOperator cert-manager Certificate operator
     * @param secretOperator Secret operator
     * @param ownerReference Owner reference for Kubernetes resources
     * @param labels Labels for Kubernetes resources
     * @param issuerRef              Reference to issuer for issuing certificates through other services like cert-manager
     */
    public CertManagerCa(Reconciliation reconciliation,
                         CaRole caRole,
                         Secret caCertSecret,
                         CaConfig caConfig,
                         CertManagerCertificateOperator certManagerCertificateOperator,
                         SecretOperator secretOperator,
                         OwnerReference ownerReference,
                         Labels labels,
                         IssuerRef issuerRef) {
        super(reconciliation, caRole, caCertSecret, null, caConfig);
        this.certManagerCertificateOperator = certManagerCertificateOperator;
        this.secretOperator = secretOperator;
        this.ownerReference = ownerReference;
        this.labels = labels;
        this.issuerRef = issuerRef;
    }

    @Override
    protected int initCaKeyGeneration(Secret caKeySecret, Secret caCertSecret) {
        if (caCertSecret != null) {
            return Annotations.intAnnotation(caCertSecret, ANNO_STRIMZI_IO_CA_KEY_GENERATION, INIT_GENERATION);
        }
        return INIT_GENERATION;
    }

    /**
     * Create or update CA data when cert-manager is managing CA.
     * <p>
     * Store the new data if it doesn't exist already, otherwise check if the certificate has changed
     * and update the data and generations accordingly.
     *
     * @param newCaCertAsBase64             New CA cert.
     * @param existingCaCertHash    Existing CA cert hash to determine if the cert has changed.
     * @param endEntityCertificate  End entity certificate to use for cert path validation.
     */
    public void createOrUpdateCa(String newCaCertAsBase64, String existingCaCertHash, X509Certificate endEntityCertificate) {
        renewalType = shouldCreateOrUpdateCa(newCaCertAsBase64, existingCaCertHash, endEntityCertificate);
        Map<String, String> updatedCertData;
        switch (renewalType) {
            case NOOP -> updatedCertData = new HashMap<>(caCertData);
            case CREATE -> {
                // No data, so we add it
                updatedCertData = new HashMap<>();
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
            }
            case RENEW_CERT -> {
                updatedCertData = new HashMap<>();
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
                ++caCertGeneration;
            }
            case REPLACE_KEY -> {
                String notAfterDate = DATE_TIME_FORMATTER.format(currentCaCertX509().getNotAfter().toInstant().atZone(ZoneId.of("Z")));
                updatedCertData = new HashMap<>();
                updatedCertData.put(Ca.SecretEntry.CRT.asKey("ca-" + notAfterDate), caCertData.get(CA_CRT));
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
                ++caCertGeneration;
                ++caKeyGeneration;
            }
            default -> throw new RuntimeException("Unsupported renewal type: " + renewalType);
        }
        caCertData = updatedCertData;
    }

    private RenewalType shouldCreateOrUpdateCa(String newCaCertAsBase64, String existingCaCertHash, X509Certificate endEntityCertificate) {
        if (caCertData.isEmpty()) {
            return RenewalType.CREATE;
        }

        X509Certificate x509CaCert;
        String newCaCertHash;
        try {
            x509CaCert = CaUtils.x509Certificate(Util.decodeBytesFromBase64(newCaCertAsBase64));
            newCaCertHash = String.format("%040x", new BigInteger(1, Util.sha1Digest(x509CaCert.getEncoded())));
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }

        if (!existingCaCertHash.equals(newCaCertHash)) {
            if (endEntityCertificate == null) {
                // Cluster operator certificate is missing, so no cert path validation to perform
                // Don't update - wait for operator cert to be available
                LOGGER.warnCr(reconciliation, "Cluster CA cert has changed, but operator certificate is missing - cannot determine if key changed. Will retry in next reconciliation.");
                return  RenewalType.NOOP;
            }
            if (CaUtils.certIsTrusted(reconciliation, List.of(endEntityCertificate), x509CaCert)) {
                // No key replacement
                return RenewalType.RENEW_CERT;
            } else {
                // key replacement
                return RenewalType.REPLACE_KEY;
            }
        } else {
            return RenewalType.NOOP;
        }
    }

    @Override
    protected boolean removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate) {
        Iterator<Map.Entry<String, String>> iter = newData.entrySet().iterator();
        List<String> removed = new ArrayList<>();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            boolean remove = predicate.test(entry);
            if (remove) {
                String certName = entry.getKey();
                LOGGER.debugCr(reconciliation, "Removing data.{} from Secret",
                        certName.replace(".", "\\."));
                iter.remove();
                removed.add(certName);
            }
        }
        return !removed.isEmpty();
    }

    @Override
    public void maybeDeleteOldCerts() {
        deleteOldCerts();
    }

    CompletionStage<CertAndKey> maybeCopyOrGenerateCert(String entityName, Subject subject, CertAndKey existingCert) {
        return generateSignedCert(entityName, subject)
                .thenApply(newCertAndKey -> {
                    if (existingCert == null) {
                        return newCertAndKey;
                    } else if (certManagerCertUpdated(existingCert, newCertAndKey)) {
                        if (CaUtils.certIsTrusted(reconciliation, CaUtils.extractCertChain(entityName, newCertAndKey.cert()), currentCaCertX509())) {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}", reconciliation.namespace(), entityName);
                            return newCertAndKey;
                        } else {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}, but not trusted yet so keeping existing certificate.", reconciliation.namespace(), entityName);
                            return existingCert;
                        }
                    } else {
                        // Certificate has not changed
                        return existingCert;
                    }
                });
    }

    /**
     * Generates a certificate signed by cert-manager CA
     *
     * @param entityName            Name of the component the Certificate is for
     * @param subject               Subject for Certificate
     * @return CompletionStage with CertAndKey
     */
    public CompletionStage<CertAndKey> generateSignedCert(String entityName, Subject subject) {
        Certificate certificate = buildCertificateResource(entityName, subject, caConfig.getValidityDays(), caConfig.getRenewalDays());
        return certManagerCertificateOperator.reconcile(reconciliation, reconciliation.namespace(), entityName, certificate)
                .thenCompose(v -> certManagerCertificateOperator.waitForReady(reconciliation, reconciliation.namespace(), entityName))
                .thenCompose(v -> secretOperator.getAsync(reconciliation.namespace(), certManagerSecretName(entityName)))
                .thenCompose(certSecret -> {
                    Objects.requireNonNull(certSecret);
                    if (certSecret.getData().get("tls.crt") == null || certSecret.getData().get("tls.key") == null) {
                        return CompletableFuture.failedFuture(new RuntimeException("No certificate data provided"));
                    }
                    return CompletableFuture.completedFuture(new CertAndKey(Util.decodeBytesFromBase64(certSecret.getData().get("tls.key")),
                            Util.decodeBytesFromBase64(certSecret.getData().get("tls.crt")), this.caCertGeneration));
                });
    }

    /**
     * Build Certificate object to give to cert-manager to generate certificate
     *
     * @param entityName            Name of the component the Certificate is for
     * @param subject               Subject for Certificate
     * @param validityDays          Validity days for Certificate
     * @param renewalDays           Renewal days for certificate
     * @return Certificate object
     */
    private Certificate buildCertificateResource(String entityName, Subject subject, int validityDays, int renewalDays) {
        String secretName = certManagerSecretName(entityName);
        CertificateBuilder certificateBuilder = new CertificateBuilder()
                .withNewMetadata()
                    .withName(entityName)
                    .withNamespace(reconciliation.namespace())
                .endMetadata()
                .withNewSpec()
                .withCommonName(subject.commonName())
                .withNewPrivateKey()
                    .withAlgorithm("RSA")
                    .withEncoding("PKCS8")
                    .withSize(2048)
                .endPrivateKey()
                .withDuration(CaUtils.convertToFabric8Duration(validityDays))
                .withRenewBefore(CaUtils.convertToFabric8Duration(renewalDays))
                .withIsCA(false)
                .withNewSubject()
                    .withOrganizations(subject.organizationName())
                .endSubject()
                .withDnsNames(new ArrayList<>(subject.dnsNames()))
                .withIpAddresses(new ArrayList<>(subject.ipAddresses()))
                .withNewIssuerRef()
                    .withName(issuerRef.getName())
                    .withKind(issuerRef.getKind().toValue())
                    .withGroup(issuerRef.getGroup())
                .endIssuerRef()
                .withSecretName(secretName)
                .withNewSecretTemplate()
                    .withLabels(labels.toMap())
                .endSecretTemplate()
                .endSpec();
        if (ownerReference != null) {
            certificateBuilder.editMetadata().withOwnerReferences(ownerReference).endMetadata();
        }
        return certificateBuilder.build();
    }

    /**
     * Get the name of the Secret managed by cert-manager, given a Strimzi managed Secret
     *
     * @param strimziSecretName Name of the Secret managed by Strimzi
     * @return Secret name to use for cert-manager managed Secret
     */
    public static String certManagerSecretName(String strimziSecretName) {
        return strimziSecretName + CERT_MANAGER_SECRET_SUFFIX;
    }

    /**
     * Checks if two certs are the same by comparing hashes
     * @param existingCertAndKey    Existing cert
     * @param newCertAndKey         New cert
     * @return Whether the cert has been updated in the new Secret
     */
    public static boolean certManagerCertUpdated(CertAndKey existingCertAndKey, CertAndKey newCertAndKey) {
        try {
            String existingCertHash = getCertificateThumbprint(CaUtils.x509Certificate(existingCertAndKey.cert()));
            String newCertHash = getCertificateThumbprint(CaUtils.x509Certificate(newCertAndKey.cert()));
            return !existingCertHash.equals(newCertHash);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates the full SHA1-hash of the server certificate which is used to track when the certificate changes.
     *
     * @param certificate   Certificate to generate the SHA1-hash for
     * @return              SHA1-Hash of the certificate or null if certSecret contains no valid X509Certificate
     */
    private static String getCertificateThumbprint(X509Certificate certificate) throws CertificateEncodingException {
        return String.format("%040x", new BigInteger(1, Util.sha1Digest(certificate.getEncoded())));
    }
}
