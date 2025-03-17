/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.IpAndDnsValidation;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.CaUtils;
import io.strimzi.operator.common.model.CertManagerCa;
import io.strimzi.operator.common.model.InternalCa;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.CertManagerUtils.certManagerCertUpdated;
import static io.strimzi.operator.common.model.CaUtils.certIsTrusted;
import static io.strimzi.operator.common.model.CaUtils.extractCertChain;

/**
 * Represents the Cluster CA
 */
public final class ClusterCaCertificateIssuer {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ClusterCaCertificateIssuer.class);

    private ClusterCaCertificateIssuer() {
       // Utility class - prevent instantiation
    }

    /**
     * Prepares the Cruise Control certificate. It either reuses the existing certificate, renews it or generates new
     * certificate if needed.
     *
     * @param namespace                             Namespace of the Kafka cluster
     * @param ca                                    CA
     * @param clusterName                           Name of the Kafka cluster
     * @param existingCertificate                   Existing certificate (or null if they do not exist yet)
     * @param ccNode                                Cruise Control node reference
     * @param isMaintenanceTimeWindowsSatisfied     Flag indicating whether we can do maintenance tasks or not
     *
     * @return Map with CertAndKey object containing the public and private key
     *
     * @throws IOException IOException is thrown when it is raised while working with the certificates
     */
    static CompletionStage<Map<String, CertAndKey>> generateCcCerts(
            Reconciliation reconciliation,
            Ca ca,
            String namespace,
            String clusterName,
            CertAndKey existingCertificate,
            NodeRef ccNode,
            boolean isMaintenanceTimeWindowsSatisfied
    ) throws IOException {
        DnsNameGenerator ccDnsGenerator = DnsNameGenerator.of(namespace, CruiseControlResources.serviceName(clusterName));

        Function<NodeRef, Subject> subjectFn = node -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(CruiseControlResources.serviceName(clusterName));

            subject.addDnsName(CruiseControlResources.serviceName(clusterName));
            subject.addDnsName(String.format("%s.%s", CruiseControlResources.serviceName(clusterName), namespace));
            subject.addDnsName(ccDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(ccDnsGenerator.serviceDnsName());
            subject.addDnsName(CruiseControlResources.serviceName(clusterName));
            subject.addDnsName("localhost");
            return subject.build();
        };

        LOGGER.debugCr(reconciliation, "Reconciling Cruise Control certificates");
        return maybeCopyOrGenerateServerCerts(
            reconciliation, ca,
            Set.of(ccNode),
            subjectFn,
            existingCertificate == null ? Map.of() : Map.of(CruiseControl.COMPONENT_TYPE, existingCertificate),
            isMaintenanceTimeWindowsSatisfied,
            false
        );
    }

    /**
     * Prepares the Kafka broker certificates. It either reuses the existing certificates, renews them or generates new
     * certificates if needed.
     *
     * @param namespace                             Namespace of the Kafka cluster
     * @param ca                                    CA
     * @param clusterName                           Name of the Kafka cluster
     * @param existingCertificates                  Existing certificates (or null if they do not exist yet)
     * @param nodes                                 Nodes that are part of the Kafka cluster
     * @param externalBootstrapAddresses            List of external bootstrap addresses (used for certificate SANs)
     * @param externalAddresses                     Map with external listener addresses for the different nodes (used for certificate SANs)
     * @param isMaintenanceTimeWindowsSatisfied     Flag indicating whether we can do maintenance tasks or not
     *
     * @return Map with CertAndKey objects containing the public and private keys for the different brokers
     *
     * @throws IOException IOException is thrown when it is raised while working with the certificates
     */
    static CompletionStage<Map<String, CertAndKey>> generateBrokerCerts(
            Reconciliation reconciliation,
            Ca ca,
            String namespace,
            String clusterName,
            Map<String, CertAndKey> existingCertificates,
            Set<NodeRef> nodes,
            Set<String> externalBootstrapAddresses,
            Map<Integer, Set<String>> externalAddresses,
            boolean isMaintenanceTimeWindowsSatisfied
    ) throws IOException {
        LOGGER.debugCr(reconciliation, "Reconciling kafka broker certificates");
        return maybeCopyOrGenerateServerCerts(
                reconciliation,
                ca,
                nodes,
                kafkaNodeCertsSubjectFn(namespace, clusterName, externalBootstrapAddresses, externalAddresses),
                existingCertificates,
                isMaintenanceTimeWindowsSatisfied,
                true
        );
    }

    private static Function<NodeRef, Subject> kafkaNodeCertsSubjectFn(String namespace, String clusterName,
                                                               Set<String> externalBootstrapAddresses,
                                                               Map<Integer, Set<String>> externalAddresses
    ) {
        return node -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(KafkaResources.kafkaComponentName(clusterName));

            subject.addDnsNames(ModelUtils.generateAllServiceDnsNames(namespace, KafkaResources.bootstrapServiceName(clusterName)));
            subject.addDnsNames(ModelUtils.generateAllServiceDnsNames(namespace, KafkaResources.brokersServiceName(clusterName)));

            subject.addDnsName(DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(clusterName), node.podName()));
            subject.addDnsName(DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(clusterName), node.podName()));

            // Controller-only nodes do not have the SANs for external listeners.
            // That helps us to avoid unnecessary rolling updates when the SANs change
            if (node.broker())    {
                if (externalBootstrapAddresses != null) {
                    for (String dnsName : externalBootstrapAddresses) {
                        if (IpAndDnsValidation.isValidIpAddress(dnsName)) {
                            subject.addIpAddress(dnsName);
                        } else {
                            subject.addDnsName(dnsName);
                        }
                    }
                }

                if (externalAddresses.get(node.nodeId()) != null) {
                    for (String dnsName : externalAddresses.get(node.nodeId())) {
                        if (IpAndDnsValidation.isValidIpAddress(dnsName)) {
                            subject.addIpAddress(dnsName);
                        } else {
                            subject.addDnsName(dnsName);
                        }
                    }
                }
            }

            return subject.build();
        };
    }

    /**
     * Copy already existing certificates from based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up).
     *
     * @param reconciliation                        Reconciliation marker
     * @param ca                                    CA
     * @param nodes                                 List of nodes for which the certificates should be generated
     * @param subjectFn                             Function to generate certificate subject for given node / pod
     * @param existingCertificates                Existing certificates (or null if they do not exist yet)
     * @param isMaintenanceTimeWindowsSatisfied     Flag indicating if we are inside a maintenance window or not
     *
     * @return Returns map with node certificates which can be used to create or update the stored certificates
     *
     * @throws IOException Throws IOException when working with files fails
     */
    /* test */ static CompletionStage<Map<String, CertAndKey>> maybeCopyOrGenerateServerCerts(
            Reconciliation reconciliation,
            Ca ca,
            Set<NodeRef> nodes,
            Function<NodeRef, Subject> subjectFn,
            Map<String, CertAndKey> existingCertificates,
            boolean isMaintenanceTimeWindowsSatisfied,
            boolean includeCaChain
    ) throws IOException {
        return switch (ca) {
            case InternalCa internalCa -> CompletableFuture.completedFuture(maybeCopyOrGenerateServerCertsWithInternalCa(reconciliation, internalCa, nodes, subjectFn, existingCertificates, isMaintenanceTimeWindowsSatisfied, includeCaChain));
            case CertManagerCa certManagerCa -> maybeCopyOrGenerateServerCertsWithCertManagerCa(reconciliation, certManagerCa, nodes, subjectFn, existingCertificates);
            default -> CompletableFuture.failedStage(new InvalidResourceException("Unable to generate server certificate for unknown type of CA {}" + ca));
        };
    }

    /**
     * Copy already existing certificates from based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up).
     *
     * @param reconciliation                        Reconciliation marker
     * @param ca                                    CA
     * @param nodes                                 List of nodes for which the certificates should be generated
     * @param subjectFn                             Function to generate certificate subject for given node / pod
     * @param existingCertificates                  Existing certificates (or null if they do not exist yet)
     *
     * @return Returns map with node certificates which can be used to create or update the stored certificates
     *
     */
    /* test */ static CompletionStage<Map<String, CertAndKey>> maybeCopyOrGenerateServerCertsWithCertManagerCa(
            Reconciliation reconciliation,
            CertManagerCa ca,
            Set<NodeRef> nodes,
            Function<NodeRef, Subject> subjectFn,
            Map<String, CertAndKey> existingCertificates
    ) {
        List<CompletableFuture<Map.Entry<String, CertAndKey>>> futureList = new ArrayList<>();
        for (NodeRef node : nodes) {
            String podName = node.podName();
            Subject subject = subjectFn.apply(node);
            CertAndKey existingCertAndKey = Optional.ofNullable(existingCertificates)
                    .map(existing -> existing.get(podName))
                    .orElse(null);

            futureList.add(maybeCopyOrGenerateCertManagerCert(reconciliation, ca, podName, subject, existingCertAndKey)
                    .thenApply(certAndKey -> Map.entry(podName, certAndKey))
                    .toCompletableFuture());
        }
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .thenApply(v -> futureList.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * Copy already existing certificates from based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up).
     *
     * @param reconciliation                        Reconciliation marker
     * @param ca                                    CA
     * @param nodes                                 List of nodes for which the certificates should be generated
     * @param subjectFn                             Function to generate certificate subject for given node / pod
     * @param existingCertificates                  Existing certificates (or null if they do not exist yet)
     * @param isMaintenanceTimeWindowsSatisfied     Flag indicating if we are inside a maintenance window or not
     *
     * @return Returns map with node certificates which can be used to create or update the stored certificates
     *
     * @throws IOException Throws IOException when working with files fails
     */
    /* test */ static Map<String, CertAndKey> maybeCopyOrGenerateServerCertsWithInternalCa(
            Reconciliation reconciliation,
            InternalCa ca,
            Set<NodeRef> nodes,
            Function<NodeRef, Subject> subjectFn,
            Map<String, CertAndKey> existingCertificates,
            boolean isMaintenanceTimeWindowsSatisfied,
            boolean includeCaChain
    ) throws IOException {
        // Maps for storing the certificates => will be used in the new or updated certificate store. This map is filled in this method and returned at the end.
        Map<String, CertAndKey> certs = new HashMap<>();

        // Temp files used when we need to generate new certificates
        File brokerCsrFile = Files.createTempFile("tls", "broker-csr").toFile();
        File brokerKeyFile = Files.createTempFile("tls", "broker-key").toFile();
        File brokerCertFile = Files.createTempFile("tls", "broker-cert").toFile();
        File brokerKeyStoreFile = Files.createTempFile("tls", "broker-p12").toFile();

        for (NodeRef node : nodes)  {
            String podName = node.podName();
            Subject subject = subjectFn.apply(node);
            CertAndKey certAndKey = Optional.ofNullable(existingCertificates)
                    .map(existing -> existing.get(podName))
                    .orElse(null);

            List<String> reasons = new ArrayList<>();

            if (certAndKey == null) {
                reasons.add("certificate doesn't exist yet for pod");
            } else if (hasCaCertGenerationChanged(certAndKey.caCertGeneration(), ca, podName)) {
                reasons.add("certificate for pod has old cert generation");
            } else {
                // A certificate for this node already exists, so we will try to reuse it
                LOGGER.debugCr(reconciliation, "certificate for node {} already exists", node);

                if (certSubjectChanged(reconciliation, certAndKey, subject, podName))   {
                    reasons.add("DNS names changed");
                }

                if (ca.isExpiring(certAndKey.cert()) && isMaintenanceTimeWindowsSatisfied)  {
                    reasons.add("certificate is expiring");
                }


                // In Strimzi 0.48 we moved to using the PEM certificates directly instead of PKCS12 in the Kafka brokers.
                // But that (unintentionally) removed the full CA chain from the server certificates. We added them back
                // in Strimzi 0.50. But this logic is needed to actually roll out the updated Secrets with the full CA chain.
                // For more details, see https://github.com/strimzi/strimzi-kafka-operator/issues/12364.
                //
                // After some time - after multiple Strimzi releases, once the CA chains are added in all clusters, we
                // should be able to remove this logic again.
                if (includeCaChain && !includesCaChain(certAndKey.cert(), ca.currentCaCertBytes())) {
                    reasons.add("CA chain added");
                }
            }

            if (!reasons.isEmpty())  {
                LOGGER.infoCr(reconciliation, "Certificate for pod {} needs to be regenerated because: {}", podName, String.join(", ", reasons));
                CertAndKey newCertAndKey = ca.generateSignedCert(subject, brokerCsrFile, brokerKeyFile, brokerCertFile, brokerKeyStoreFile, includeCaChain);
                certs.put(podName, newCertAndKey);
            }   else {
                certs.put(podName, certAndKey);
            }
        }

        // Delete the temp files used to generate new certificates
        delete(reconciliation, brokerCsrFile);
        delete(reconciliation, brokerKeyFile);
        delete(reconciliation, brokerCertFile);
        delete(reconciliation, brokerKeyStoreFile);

        return certs;
    }

    private static void delete(Reconciliation reconciliation, File file) {
        if (file != null && !file.delete()) {
            LOGGER.warnCr(reconciliation, "{} cannot be deleted", file.getName());
        }
    }


    /**
     * Generates or reuses a single certificate signed by this Cluster CA.
     * Used for components that only act as clients, like Entity Operators and Kafka Exporter.
     *
     * @param reconciliation                        Reconciliation marker
     * @param commonName                            Common Name for the certificate
     * @param ca                                    CA
     * @param existingCertAndKey                    Existing certificate (or null if none exists)
     * @param isMaintenanceTimeWindowsSatisfied     Whether we are in a maintenance window
     *
     * @return CertAndKey object containing the certificate and key with CA generation set
     */
    public static CompletionStage<CertAndKey> maybeCopyOrGenerateClientCert(
            Reconciliation reconciliation,
            String commonName,
            Ca ca,
            CertAndKey existingCertAndKey,
            boolean isMaintenanceTimeWindowsSatisfied
    ) {
        return switch (ca) {
            case InternalCa internalCa -> CompletableFuture.completedFuture(maybeCopyOrGenerateClientCertWithInternalCa(reconciliation, commonName, internalCa, existingCertAndKey, isMaintenanceTimeWindowsSatisfied));
            case CertManagerCa certManagerCa -> maybeCopyOrGenerateClientCertWithCertManagerCa(reconciliation, commonName, certManagerCa, existingCertAndKey);
            default -> CompletableFuture.failedStage(new InvalidResourceException("Unable to generate server certificate for unknown type of CA {}" + ca));
        };
    }


    /* test */ static CertAndKey maybeCopyOrGenerateClientCertWithInternalCa(
            Reconciliation reconciliation,
            String commonName,
            InternalCa ca,
            CertAndKey existingCertAndKey,
            boolean isMaintenanceTimeWindowsSatisfied
    ) {
        List<String> reasons = new ArrayList<>();

        if (existingCertAndKey == null) {
            reasons.add("certificate doesn't exist yet");
        } else if (hasCaCertGenerationChanged(existingCertAndKey.caCertGeneration(), ca, commonName)) {
            reasons.add("certificate has old cert generation");
        } else {
            // Certificate exists and CA generation matches - check if renewal is needed
            if (ca.isExpiring(existingCertAndKey.cert()) && isMaintenanceTimeWindowsSatisfied) {
                reasons.add("certificate is expiring");
            }
        }

        CertAndKey certAndKey = null;
        if (!reasons.isEmpty()) {
            LOGGER.infoCr(reconciliation, "Certificate for component {} needs to be regenerated because: {}", commonName, String.join(", ", reasons));

            try {
                certAndKey = ca.getSignedCert(commonName, InternalCa.IO_STRIMZI);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
        } else {
            certAndKey = existingCertAndKey;
        }

        return certAndKey;
    }


    /* test */ static CompletionStage<CertAndKey> maybeCopyOrGenerateClientCertWithCertManagerCa(
            Reconciliation reconciliation,
            String commonName,
            CertManagerCa ca,
            CertAndKey existingCertAndKey
    ) {
        Subject subject = CaUtils.getSubject(commonName, InternalCa.IO_STRIMZI);
        return maybeCopyOrGenerateCertManagerCert(reconciliation, ca, commonName, subject, existingCertAndKey);
    }

    /**
     * It checks if the current CA certificate generation is changed compared to the one
     * that signed the CertAndKey.
     */
    private static boolean hasCaCertGenerationChanged(int certAndKeyCaCertGeneration, Ca ca, String podName) {
        LOGGER.debugOp("Pod {} generation anno = {}, current CA generation = {}", podName, certAndKeyCaCertGeneration, ca.caCertGeneration());
        return certAndKeyCaCertGeneration != ca.caCertGeneration();
    }


    /**
     * Checks if the CA chain is contained at the end of the certificate.
     *
     * @param cert      The server certificate as a byte array
     * @param caChain   The CA chain as a byte array
     *
     * @return  True if the CA chain is included at the end of the certificate, false otherwise.
     */
    /* test */ static boolean includesCaChain(byte[] cert, byte[] caChain) {
        if (cert == null || caChain == null || cert.length < caChain.length) {
            // The CA chain is definitely not included
            return false;
        } else {
            return Arrays.equals(Arrays.copyOfRange(cert, cert.length - caChain.length, cert.length), caChain);
        }
    }

    /**
     * Generates a certificate using cert-manager and selects which certificate to use.
     * Compares the existing certificate with the newly generated one and decides whether to use the new one.
     *
     * @param reconciliation        Reconciliation marker
     * @param ca                    cert-manager CA
     * @param certName              Name of the certificate (pod name or component name)
     * @param subject               Certificate subject with DNS names, IP addresses, etc.
     * @param existingCertAndKey    Existing certificate (or null if none exists)
     * @return CompletionStage with the certificate to use (either new or existing)
     */
    private static CompletionStage<CertAndKey> maybeCopyOrGenerateCertManagerCert(
            Reconciliation reconciliation,
            CertManagerCa ca,
            String certName,
            Subject subject,
            CertAndKey existingCertAndKey
    ) {
        return ca.generateSignedCert(certName, subject)
                .thenApply(newCertAndKey -> {
                    if (existingCertAndKey == null) {
                        return newCertAndKey;
                    } else if (certManagerCertUpdated(existingCertAndKey, newCertAndKey)) {
                        if (certIsTrusted(reconciliation, extractCertChain(certName, newCertAndKey.cert()), ca.currentCaCertX509())) {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}", reconciliation.namespace(), certName);
                            return newCertAndKey;
                        } else {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}, but not trusted yet so keeping existing certificate.", reconciliation.namespace(), certName);
                            return existingCertAndKey;
                        }
                    } else {
                        // Certificate has not changed
                        return existingCertAndKey;
                    }
                });
    }

    /**
     * Checks whether subject alternate names changed and certificate needs a renewal
     *
     * @param certAndKey        Current certificate
     * @param desiredSubject    Desired subject alternate names
     * @param podName           Name of the pod to which this certificate belongs (used for log messages)
     *
     * @return  True if the subjects are different, false otherwise
     */
    /* test */
    static boolean certSubjectChanged(Reconciliation reconciliation, CertAndKey certAndKey, Subject desiredSubject, String podName)    {
        Collection<String> desiredAltNames = desiredSubject.subjectAltNames().values();
        Collection<String> currentAltNames = getSubjectAltNames(reconciliation, certAndKey.cert());

        if (currentAltNames != null && desiredAltNames.containsAll(currentAltNames) && currentAltNames.containsAll(desiredAltNames))   {
            LOGGER.traceCr(reconciliation, "Alternate subjects match. No need to refresh cert for pod {}.", podName);
            return false;
        } else {
            LOGGER.infoCr(reconciliation, "Alternate subjects for pod {} differ", podName);
            LOGGER.infoCr(reconciliation, "Current alternate subjects: {}", currentAltNames);
            LOGGER.infoCr(reconciliation, "Desired alternate subjects: {}", desiredAltNames);
            return true;
        }
    }

    /**
     * Extracts the alternate subject names out of existing certificate
     *
     * @param certificate   Existing X509 certificate as a byte array
     *
     * @return  List of certificate Subject Alternate Names
     */
    private static List<String> getSubjectAltNames(Reconciliation reconciliation, byte[] certificate) {
        List<String> subjectAltNames = null;

        try {
            X509Certificate cert = CaUtils.x509Certificate(certificate);
            Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
            subjectAltNames = altNames.stream()
                    .filter(name -> name.get(1) instanceof String)
                    .map(item -> (String) item.get(1))
                    .collect(Collectors.toList());
        } catch (CertificateException | RuntimeException e) {
            // TODO: We should mock the certificates properly so that this doesn't fail in tests (not now => long term :-o)
            LOGGER.debugCr(reconciliation, "Failed to parse existing certificate", e);
        }

        return subjectAltNames;
    }
}
