/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * Creates HTTP client and interacts with Kafka Agent's REST endpoint
 */
class KafkaAgentClient {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAgentClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state/";
    private static final String NODE_CONFIG_REST_PATH = "/v1/node-config/";
    private static final int KAFKA_AGENT_HTTPS_PORT = 8443;

    private String namespace;
    private Reconciliation reconciliation;
    private String cluster;
    private Secret clusterCaCertSecret;
    private Secret coKeySecret;
    private HttpClient httpClient;

    KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace, Secret clusterCaCertSecret, Secret coKeySecret) {
        this.reconciliation = reconciliation;
        this.cluster = cluster;
        this.namespace = namespace;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.httpClient = createHttpClient();
    }

    /* test */ KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace) {
        this.reconciliation = reconciliation;
        this.namespace = namespace;
        this.cluster =  cluster;
    }

    private HttpClient createHttpClient() {
        if (clusterCaCertSecret == null || coKeySecret == null) {
            throw new RuntimeException("Missing secrets for cluster CA and operator certificates required to create connection to Kafka Agent");
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(new ByteArrayInputStream(
                            Util.decodeFromSecret(clusterCaCertSecret, "ca.p12")),
                    new String(Util.decodeFromSecret(clusterCaCertSecret, "ca.password"), StandardCharsets.UTF_8).toCharArray());

            String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
            trustManagerFactory.init(trustStore);
            char[] keyPassword = new String(Util.decodeFromSecret(coKeySecret, "cluster-operator.password"), StandardCharsets.UTF_8).toCharArray();
            KeyStore coKeyStore = KeyStore.getInstance("PKCS12");
            coKeyStore.load(new ByteArrayInputStream(
                            Util.decodeFromSecret(coKeySecret, "cluster-operator.p12")),
                    keyPassword
            );

            String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
            keyManagerFactory.init(coKeyStore, keyPassword);

            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            return HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to configure HTTP client", e);
        }
    }

    String doGet(URI uri) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            var response = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Unexpected HTTP status code: " + response.statusCode());
            }
            return response.body();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to send HTTP request to Kafka Agent", e);
        }
    }

    /**
     * Gets broker state by sending HTTP request to the /v1/broker-state endpoint of the KafkaAgent
     *
     * @param podName Name of the pod to interact with
     * @return A BrokerState that contains broker state and recovery progress.
     *         -1 is returned for broker state if the http request failed or returned non 200 response.
     *         Null value is returned for recovery progress if broker state is not 2 (RECOVERY).
     */
    BrokerState getBrokerState(String podName) {
        BrokerState brokerstate = new BrokerState(-1, null);
        String host = DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName);
        try {
            URI uri = new URI("https", null, host, KAFKA_AGENT_HTTPS_PORT, BROKER_STATE_REST_PATH, null, null);
            brokerstate = MAPPER.readValue(doGet(uri), BrokerState.class);
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to parse broker state", e);
        } catch (URISyntaxException e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state due to invalid URI", e);
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state", e);
        }
        return brokerstate;
    }

    /**
     * Gets node configuration by sending HTTP request to the /v1/node-config endpoint of the KafkaAgent
     *
     * @param podName Name of the pod to interact with
     * @return a Config instance containing the node configuration
     */
    Config getNodeConfiguration(String podName) {
        Config config = null;
        String host = DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName);
        try {
            URI uri = new URI("https", null, host, KAFKA_AGENT_HTTPS_PORT, NODE_CONFIG_REST_PATH, null, null);
            Properties nodeConfig = MAPPER.readValue(doGet(uri), Properties.class);
            List<ConfigEntry> configEntryList = new ArrayList<>();
            nodeConfig.stringPropertyNames()
                    .forEach(key -> configEntryList.add(new ConfigEntry(key, nodeConfig.getProperty(key))));
            config = new Config(configEntryList);
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to parse node configuration", e);
        } catch (URISyntaxException e) {
            LOGGER.warnCr(reconciliation, "Failed to get node configuration due to invalid URI", e);
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "Failed to get node configuration", e);
        }
        return config;
    }
}
